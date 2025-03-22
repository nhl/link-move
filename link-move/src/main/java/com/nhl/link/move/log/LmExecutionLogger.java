package com.nhl.link.move.log;

import com.nhl.link.move.RowAttribute;
import org.apache.cayenne.Persistent;
import org.dflib.Series;

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

    void extractorStarted(RowAttribute[] header, Object query);

    void targetFilterApplied(Object filter);

    void segmentStarted();

    void deleteSegmentFinished(int objectsProcessed, Series<? extends Persistent> objectsDeleted);

    void createSegmentFinished(int rowsProcessed, Series<? extends Persistent> objectsInserted);

    void createOrUpdateSegmentFinished(int rowsProcessed, Series<? extends Persistent> objectsInserted, Series<? extends Persistent> objectsUpdated);

    void sourceKeysSegmentFinished(int rowsProcessed, Set<?> keysExtracted);

}
