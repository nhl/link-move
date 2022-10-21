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

import java.util.Map;
import java.util.Objects;

/**
 * A task that reads streamed source data and creates/updates records in a
 * target DB.
 *
 * @since 1.3
 */
public class CreateOrUpdateTask<T extends DataObject> extends BaseTask {

    private final ExtractorName extractorName;
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

        super(tokenManager, logger);

        this.extractorName = extractorName;
        this.batchSize = batchSize;
        this.targetCayenneService = targetCayenneService;
        this.extractorService = extractorService;
        this.processor = processor;
    }

    @Override
    protected Execution doRun(Map<String, ?> params) {

        Objects.requireNonNull(params, "Null params");

        try (Execution execution = createExecution(extractorName, params)) {

            try (RowReader data = getRowReader(params)) {
                BatchProcessor<Object[]> batchProcessor = createBatchProcessor(execution, data.getHeader());
                BatchRunner.create(batchProcessor).withBatchSize(batchSize).run(data);
            }

            return execution;
        }
    }

    protected BatchProcessor<Object[]> createBatchProcessor(Execution execution, RowAttribute[] rowHeader) {
        ObjectContext context = targetCayenneService.newContext();
        return rows -> processor.process(
                execution,
                new CreateOrUpdateSegment<>(context, rowHeader, srcRowsAsDataFrame(rowHeader, rows)));
    }

    protected RowReader getRowReader(Map<String, ?> extractorParams) {
        return extractorService.getExtractor(extractorName).getReader(extractorParams);
    }
}
