package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.batch.BatchProcessor;
import com.nhl.link.move.batch.BatchRunner;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.BaseTask;
import com.nhl.link.move.runtime.token.ITokenManager;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

/**
 * An {@link LmTask} that extracts all the keys from the source data store. The
 * result is stored in memory in the Execution object.
 *
 * @since 1.3
 */
public class SourceKeysTask extends BaseTask {

    private static final String EXEC_LABEL = SourceKeysTask.class.getSimpleName();

    public static final String RESULT_KEY = SourceKeysTask.class.getName() + ".RESULT";

    private final int batchSize;
    private final IExtractorService extractorService;
    private final ExtractorName sourceExtractorName;
    private final SourceKeysSegmentProcessor processor;

    public SourceKeysTask(
            ExtractorName sourceExtractorName,
            int batchSize,
            IExtractorService extractorService,
            ITokenManager tokenManager,
            SourceKeysSegmentProcessor processor) {

        super(tokenManager);

        this.sourceExtractorName = sourceExtractorName;
        this.batchSize = batchSize;
        this.extractorService = extractorService;
        this.processor = processor;
    }

    @Override
    protected Execution doRun(Map<String, ?> params) {

        Objects.requireNonNull(params, "Null params");

        try (Execution execution = new Execution(EXEC_LABEL, sourceExtractorName, params)) {

            execution.setAttribute(RESULT_KEY, new HashSet<>());

            try (RowReader data = getRowReader(params)) {
                BatchProcessor<Object[]> batchProcessor = createBatchProcessor(execution, data.getHeader());
                BatchRunner.create(batchProcessor).withBatchSize(batchSize).run(data);
            }

            return execution;
        }
    }

    protected BatchProcessor<Object[]> createBatchProcessor(Execution execution, RowAttribute[] rowHeader) {
        Index columns = toIndex(rowHeader);
        return rows -> processor.process(execution, new SourceKeysSegment(rowHeader, DataFrame.newFrame(columns).objectsToRows(rows, r -> r)));
    }

    /**
     * Returns a RowReader obtained from a named extractor and wrapped in a read stats counter.
     */
    protected RowReader getRowReader(Map<String, ?> extractorParams) {
        return extractorService.getExtractor(sourceExtractorName).getReader(extractorParams);
    }
}
