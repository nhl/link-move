package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.Execution;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.batch.BatchProcessor;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.BaseTask;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

/**
 * @param <T>
 * @since 2.6
 */
public class CreateTask<T extends DataObject> extends BaseTask {

    private final ITargetCayenneService targetCayenneService;
    private final IExtractorService extractorService;
    private final CreateSegmentProcessor processor;

    public CreateTask(
            ExtractorName extractorName,
            int batchSize,
            ITargetCayenneService targetCayenneService,
            IExtractorService extractorService,
            ITokenManager tokenManager,
            CreateSegmentProcessor processor,
            LmLogger logger) {

        super(extractorName, batchSize, tokenManager, logger);

        this.targetCayenneService = targetCayenneService;
        this.extractorService = extractorService;
        this.processor = processor;
    }

    @Override
    protected String createLabel() {
        return "create";
    }

    @Override
    protected void doRun(Execution exec) {

        try (RowReader data = getRowReader(exec)) {
            BatchProcessor<Object[]> batchProcessor = createBatchProcessor(exec, data.getHeader());
            createBatchRunner(batchProcessor).run(data);
        }
    }

    protected BatchProcessor<Object[]> createBatchProcessor(Execution execution, RowAttribute[] rowHeader) {
        ObjectContext context = targetCayenneService.newContext();
        return rows -> processor.process(execution, new CreateSegment(context, rowHeader, srcRowsAsDataFrame(rowHeader, rows)));
    }

    protected RowReader getRowReader(Execution exec) {
        return extractorService.getExtractor(exec.getExtractorName()).getReader(exec.getParameters());
    }
}
