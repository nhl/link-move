package com.nhl.link.move.runtime.task.createorupdate;

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
import org.apache.cayenne.ObjectContext;

/**
 * A task that reads streamed source data and creates/updates records in a
 * target DB.
 *
 * @since 1.3
 */
public class CreateOrUpdateTask extends BaseTask {

    private final ITargetCayenneService targetCayenneService;
    private final IExtractorService extractorService;
    private final CreateOrUpdateSegmentProcessor processor;

    public CreateOrUpdateTask(
            ExtractorName extractorName,
            int batchSize,
            ITargetCayenneService targetCayenneService,
            IExtractorService extractorService,
            ITokenManager tokenManager,
            CreateOrUpdateSegmentProcessor processor,
            LmLogger logger) {

        super(extractorName, batchSize, tokenManager, logger);

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
            createBatchRunner(batchProcessor).run(data);
        }
    }

    @Override
    protected void onExecFinished(Execution exec) {
        logger.createOrUpdateExecFinished(exec);
    }

    protected BatchProcessor<Object[]> createBatchProcessor(Execution execution, RowAttribute[] rowHeader) {
        ObjectContext context = targetCayenneService.newContext();
        return rows -> processor.process(
                execution,
                new CreateOrUpdateSegment(context, rowHeader).setSourceRows(srcRowsAsDataFrame(rowHeader, rows)));
    }

    protected RowReader getRowReader(Execution exec) {
        return extractorService.getExtractor(exec.getExtractorName()).getReader(exec.getParameters());
    }
}
