package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.CountingRowReader;
import com.nhl.link.move.Execution;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.batch.BatchProcessor;
import com.nhl.link.move.batch.BatchRunner;
import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.BaseTask;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

import java.util.Map;

/**
 * A task that reads streamed source data and creates/updates records in a
 * target DB.
 *
 * @since 1.3
 */
public class CreateOrUpdateTask<T extends DataObject> extends BaseTask {

    private ExtractorName extractorName;
    private int batchSize;
    private ITargetCayenneService targetCayenneService;
    private IExtractorService extractorService;
    private CreateOrUpdateSegmentProcessor<T> processor;

    public CreateOrUpdateTask(
            ExtractorName extractorName,
            int batchSize,
            ITargetCayenneService targetCayenneService,
            IExtractorService extractorService,
            ITokenManager tokenManager,
            CreateOrUpdateSegmentProcessor<T> processor) {

        super(tokenManager);

        this.extractorName = extractorName;
        this.batchSize = batchSize;
        this.targetCayenneService = targetCayenneService;
        this.extractorService = extractorService;
        this.processor = processor;
    }

    @Override
    protected Execution doRun(Map<String, ?> params) {

        if (params == null) {
            throw new NullPointerException("Null params");
        }

        try (Execution execution = new Execution("CreateOrUpdateTask:" + extractorName, params);) {

            try (RowReader data = getRowReader(execution, params)) {
                BatchProcessor batchProcessor = createBatchProcessor(execution, data.getHeader());
                BatchRunner.create(batchProcessor).withBatchSize(batchSize).run(data);
            }

            return execution;
        }
    }

    protected BatchProcessor<Object[]> createBatchProcessor(Execution execution, RowAttribute[] rowHeader) {
        ObjectContext context = targetCayenneService.newContext();
        Index columns = toIndex(rowHeader);
        return rows -> processor.process(execution,
                new CreateOrUpdateSegment<>(context, rowHeader, DataFrame.newFrame(columns).objectsToRows(rows, r -> r)));
    }

    /**
     * Returns a RowReader obtained from a named extractor and wrapped in a read
     * stats counter.
     */
    protected RowReader getRowReader(Execution execution, Map<String, ?> extractorParams) {
        Extractor extractor = extractorService.getExtractor(extractorName);
        return new CountingRowReader(extractor.getReader(extractorParams), execution.getStats());
    }
}
