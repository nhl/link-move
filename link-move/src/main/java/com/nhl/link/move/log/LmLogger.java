package com.nhl.link.move.log;

import com.nhl.link.move.Execution;

import java.util.Set;

/**
 * @since 3.0
 */
public interface LmLogger {

    void execStarted(Execution exec);

    void deleteExecFinished(Execution exec);

    void createExecFinished(Execution exec);

    void createOrUpdateExecFinished(Execution exec);

    void sourceKeysExecFinished(Execution exec);

    void segmentStarted(Execution exec);

    void deleteSegmentFinished(Execution exec, int objectsProcessed, int objectsDeleted);

    void createSegmentFinished(Execution exec, int rowsProcessed, int objectsInserted);

    void createOrUpdateSegmentFinished(Execution exec, int rowsProcessed, int objectsInserted, int objectsUpdated);

    void sourceKeysSegmentFinished(Execution exec, int rowsProcessed, Set<?> keysExtracted);
}
