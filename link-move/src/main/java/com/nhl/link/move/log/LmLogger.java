package com.nhl.link.move.log;

import com.nhl.link.move.Execution;

/**
 * @since 3.0
 */
public interface LmLogger {

    void batchStarted(Execution exec);

    void deleteBatchFinished(Execution exec, int objectsProcessed, int objectsDeleted);

    void createBatchFinished(Execution exec, int rowsProcessed, int objectsInserted);

    void createOrUpdateBatchFinished(Execution exec, int rowsProcessed, int objectsInserted, int objectsUpdated);

    void sourceKeysBatchFinished(Execution exec, int rowsProcessed, int keysExtracted);
}