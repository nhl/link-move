package com.nhl.link.move.runtime.task;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.SyncToken;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import com.nhl.link.move.runtime.token.ITokenManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @since 1.3
 */
public abstract class BaseTask implements LmTask {

    @Deprecated(since = "3.0")
    private ITokenManager tokenManager;

    public BaseTask(ITokenManager tokenManager) {
        this.tokenManager = tokenManager;
    }

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

    @Override
    // tagged as final to help upgrading to 2.8 ... users must override "doRun" instead
    public final Execution run(Map<String, ?> params) {
        return doRun(params);
    }

    @Override
    public Execution run(SyncToken token, Map<String, ?> params) {

        Map<String, ?> runParams = token != null ? mergeParams(token, params) : params;
        Execution exec = doRun(runParams);

        if (token != null) {
            // if we ever start using delayed executions, token should be saved inside the execution...
            tokenManager.saveToken(token);
        }

        return exec;
    }

    /**
     * @since 2.8
     */
    protected abstract Execution doRun(Map<String, ?> params);

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
