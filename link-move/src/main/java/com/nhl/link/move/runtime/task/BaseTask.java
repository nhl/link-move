package com.nhl.link.move.runtime.task;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.batch.BatchProcessor;
import com.nhl.link.move.batch.BatchRunner;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;
import org.dflib.DataFrame;
import org.dflib.Index;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @since 1.3
 */
public abstract class BaseTask implements LmTask {

    private static final AtomicLong execIdGenerator = new AtomicLong();

    protected final LmLogger logger;

    private final String label;
    private final ExtractorName extractorName;
    private final int batchSize;

    /**
     * @since 3.0.0
     */
    protected static DataFrame srcRowsAsDataFrame(RowAttribute[] rowHeader, List<Object[]> rows) {
        return DataFrame.byArrayRow(toIndex(rowHeader)).ofIterable(rows);
    }

    protected static Index toIndex(RowAttribute[] rowHeader) {
        String[] columns = new String[rowHeader.length];

        for (int i = 0; i < rowHeader.length; i++) {
            columns[i] = rowHeader[i].getSourceName();
        }

        return Index.of(columns);
    }

    public BaseTask(ExtractorName extractorName, int batchSize, LmLogger logger) {
        this.extractorName = extractorName;
        this.batchSize = batchSize;
        this.logger = logger;
        this.label = createLabel();
    }

    @Override
    public Execution run(Map<String, ?> params) {
        return run(params, null);
    }

    @Override
    public Execution run(Map<String, ?> params, Execution parentExec) {
        Execution exec = createExec(params, parentExec);
        onExecStarted(exec);

        try {
            doRun(exec);
        } finally {
            exec.stop();
            onExecFinished(exec);
        }

        return exec;
    }

    protected String createLabel() {
        return getClass().getSimpleName();
    }

    protected Execution createExec(Map<String, ?> params, Execution parentExec) {
        // inherit the ID from parent
        long execId = parentExec != null ? parentExec.getId() : execIdGenerator.getAndIncrement();
        return new Execution(execId, label, extractorName, params, logger, parentExec);
    }

    protected <S> BatchRunner<S> createBatchRunner(BatchProcessor<S> processor) {
        return BatchRunner.create(processor).withBatchSize(batchSize);
    }

    /**
     * @since 3.0.0
     */
    protected void onExecStarted(Execution exec) {
        exec.getLogger().execStarted();
    }

    /**
     * @since 3.0.0
     */
    protected abstract void doRun(Execution exec);

    /**
     * @since 3.0.0
     */
    protected abstract void onExecFinished(Execution exec);
}
