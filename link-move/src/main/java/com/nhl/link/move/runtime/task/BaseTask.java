package com.nhl.link.move.runtime.task;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.SyncToken;
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

    private static final AtomicLong idGenerator = new AtomicLong();

    private final LmLogger logger;

    @Deprecated(since = "3.0")
    private final ITokenManager tokenManager;
    private final String label;
    private final ExtractorName extractorName;

    /**
     * @since 3.0
     */
    protected static DataFrame srcRowsAsDataFrame(RowAttribute[] rowHeader, List<Object[]> rows) {
        return DataFrame.newFrame(toIndex(rowHeader)).objectsToRows(rows, r -> r);
    }

    protected static Index toIndex(RowAttribute[] rowHeader) {
        String[] columns = new String[rowHeader.length];

        for (int i = 0; i < rowHeader.length; i++) {
            columns[i] = rowHeader[i].getSourceName();
        }

        return Index.forLabels(columns);
    }

    public BaseTask(ExtractorName extractorName, ITokenManager tokenManager, LmLogger logger) {
        this.tokenManager = tokenManager;
        this.logger = logger;
        this.extractorName = extractorName;
        this.label = createLabel();
    }

    @Override
    public Execution run(Map<String, ?> params) {
        return run(params, null);
    }

    @Override
    public Execution run(Map<String, ?> params, Execution parentExec) {
        try (Execution execution = createExec(params, parentExec)) {
            doRun(execution);
            return execution;
        }
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
        // inherit ID from parent
        long id = parentExec != null ? parentExec.getId() : idGenerator.getAndIncrement();
        return new Execution(id, label, extractorName, params, logger);
    }

    /**
     * @since 3.0
     */
    protected abstract void doRun(Execution exec);

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
