package com.nhl.link.move.runtime.task;

import org.dflib.DataFrame;
import org.dflib.Index;
import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.SyncToken;
import com.nhl.link.move.batch.BatchProcessor;
import com.nhl.link.move.batch.BatchRunner;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import com.nhl.link.move.runtime.token.ITokenManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @since 1.3
 */
public abstract class BaseTask implements LmTask {

    private static final AtomicLong execIdGenerator = new AtomicLong();

    protected final LmLogger logger;

    @Deprecated(since = "3.0")
    private final ITokenManager tokenManager;
    private final String label;
    private final ExtractorName extractorName;
    private final int batchSize;

    /**
     * @since 3.0
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

    public BaseTask(ExtractorName extractorName, int batchSize, ITokenManager tokenManager, LmLogger logger) {
        this.extractorName = extractorName;
        this.batchSize = batchSize;
        this.tokenManager = tokenManager;
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

    @Override
    public Execution run(SyncToken token, Map<String, ?> params) {

        Execution exec = run(token != null ? mergeParams(token, params) : params);

        if (token != null) {
            // if we ever start using delayed executions, token should be saved inside the execution...
            tokenManager.saveToken(token);
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
     * @since 3.0
     */
    protected void onExecStarted(Execution exec) {
        exec.getLogger().execStarted();
    }

    /**
     * @since 3.0
     */
    protected abstract void doRun(Execution exec);

    /**
     * @since 3.0
     */
    protected abstract void onExecFinished(Execution exec);

    @Deprecated(since = "3.0")
    protected Map<String, Object> mergeParams(SyncToken token, Map<String, ?> params) {
        Objects.requireNonNull(token);

        Map<String, Object> combinedParams = new HashMap<>();

        SyncToken startToken = tokenManager.previousToken(token);
        combinedParams.put(LmRuntimeBuilder.START_TOKEN_VAR, startToken.getValue());
        combinedParams.put(LmRuntimeBuilder.END_TOKEN_VAR, token.getValue());

        combinedParams.putAll(params);
        return combinedParams;
    }
}
