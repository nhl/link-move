package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.CountingRowReader;
import com.nhl.link.move.Execution;
import com.nhl.link.move.Row;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.batch.BatchProcessor;
import com.nhl.link.move.batch.BatchRunner;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.BaseTask;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

import java.util.List;
import java.util.Map;

/**
 * @param <T>
 * @since 2.6
 */
public class CreateTask<T extends DataObject> extends BaseTask {

    private ExtractorName extractorName;
    private int batchSize;
    private ITargetCayenneService targetCayenneService;
    private IExtractorService extractorService;
    private CreateSegmentProcessor<T> processor;

    public CreateTask(
            ExtractorName extractorName,
            int batchSize,
            ITargetCayenneService targetCayenneService,
            IExtractorService extractorService,
            ITokenManager tokenManager,
            CreateSegmentProcessor<T> processor) {

        super(tokenManager);

        this.extractorName = extractorName;
        this.batchSize = batchSize;
        this.targetCayenneService = targetCayenneService;
        this.extractorService = extractorService;
        this.processor = processor;
    }

    @Override
    public Execution run(Map<String, ?> params) {

        if (params == null) {
            throw new NullPointerException("Null params");
        }

        try (Execution execution = new Execution("CreateTask:" + extractorName, params)) {

            BatchProcessor<Row> batchProcessor = createBatchProcessor(execution);

            try (RowReader data = getRowReader(execution, params)) {
                BatchRunner.create(batchProcessor).withBatchSize(batchSize).run(data);
            }

            return execution;
        }
    }

    protected BatchProcessor<Row> createBatchProcessor(final Execution execution) {
        return new BatchProcessor<Row>() {

            ObjectContext context = targetCayenneService.newContext();

            @Override
            public void process(List<Row> rows) {
                processor.process(execution, new CreateSegment<T>(context, rows));
            }
        };
    }

    /**
     * Returns a RowReader obtained from a named extractor and wrapped in a read stats counter.
     */
    protected RowReader getRowReader(Execution execution, Map<String, ?> extractorParams) {
        Extractor extractor = extractorService.getExtractor(extractorName);
        return new CountingRowReader(extractor.getReader(extractorParams), execution.getStats());
    }
}
