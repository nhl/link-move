package com.nhl.link.move.log;

import com.nhl.dflib.Series;
import org.apache.cayenne.Persistent;

import java.util.Set;

/**
 * @since 3.0
 */
public interface LmExecutionLogger {

    void execStarted();

    void deleteExecFinished();

    void createExecFinished();

    void createOrUpdateExecFinished();

    void sourceKeysExecFinished();

    void segmentStarted();

    void deleteSegmentFinished(int objectsProcessed, Series<? extends Persistent> objectsDeleted);

    void createSegmentFinished(int rowsProcessed, int objectsInserted);

    void createOrUpdateSegmentFinished(int rowsProcessed, int objectsInserted, int objectsUpdated);

    void sourceKeysSegmentFinished(int rowsProcessed, Set<?> keysExtracted);
}
