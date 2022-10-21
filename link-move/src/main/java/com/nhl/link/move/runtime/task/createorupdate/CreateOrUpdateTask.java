package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.Execution;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.batch.BatchProcessor;
import com.nhl.link.move.batch.BatchRunner;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.BaseTask;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

/**
 * A task that reads streamed source data and creates/updates records in a
 * target DB.
 *
 * @since 1.3
 */
public class CreateOrUpdateTask<T extends DataObject> extends BaseTask {

    private final int batchSize;
    private final ITargetCayenneService targetCayenneService;
    private final IExtractorService extractorService;
    private final CreateOrUpdateSegmentProcessor<T> processor;

    public CreateOrUpdateTask(
            ExtractorName extractorName,
            int batchSize,
            ITargetCayenneService targetCayenneService,
            IExtractorService extractorService,
            ITokenManager tokenManager,
            CreateOrUpdateSegmentProcessor<T> processor,
            LmLogger logger) {

        super(extractorName, tokenManager, logger);

        this.batchSize = batchSize;
        this.targetCayenneService = targetCayenneService;
        this.extractorService = extractorService;
        this.processor = processor;
    }

    @Override
    protected String createLabel() {
        return "create-or-update";
    }

    @Override
    protected void doRun(Execution exec) {

        try (RowReader data = getRowReader(exec)) {
            BatchProcessor<Object[]> batchProcessor = createBatchProcessor(exec, data.getHeader());
            BatchRunner.create(batchProcessor).withBatchSize(batchSize).run(data);
        }
    }

    protected BatchProcessor<Object[]> createBatchProcessor(Execution execution, RowAttribute[] rowHeader) {
        ObjectContext context = targetCayenneService.newContext();
        return rows -> processor.process(
                execution,
                new CreateOrUpdateSegment<>(context, rowHeader, srcRowsAsDataFrame(rowHeader, rows)));
    }

    protected RowReader getRowReader(Execution exec) {
        return extractorService.getExtractor(exec.getExtractorName()).getReader(exec.getParameters());
    }
}
