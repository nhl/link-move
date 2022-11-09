package com.nhl.link.move.runtime.task.common;

import com.nhl.dflib.BooleanSeries;
import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Series;
import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.create.CreateSegment;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;
import com.nhl.link.move.runtime.task.delete.DeleteSegment;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysSegment;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysTask;
import org.apache.cayenne.Persistent;

import java.util.Collections;
import java.util.Set;

public class StatsIncrementor {
    private static final StatsIncrementor instance = new StatsIncrementor();

    public static StatsIncrementor instance() {
        return instance;
    }


    public void sourceRowsExtracted(Execution e, CreateSegment segment) {
        doSourceRowsExtracted(e, segment.getSourceRows());
    }

    public void sourceRowsExtracted(Execution e, SourceKeysSegment segment) {
        doSourceRowsExtracted(e, segment.getSourceRows());
    }

    public void sourceRowsExtracted(Execution e, CreateOrUpdateSegment segment) {
        doSourceRowsExtracted(e, segment.getSourceRows());
    }

    private static void doSourceRowsExtracted(Execution e, DataFrame sourceRows) {
        e.getLogger().segmentStarted();
        e.getStats().incrementExtracted(sourceRows.height());
    }

    public void targetsCommitted(Execution e, CreateSegment segment) {
        e.getStats().incrementCreated(segment.getMapped().height());

        // call the logger before incrementing the batch count, so that start and end batch numbers match
        e.getLogger().createSegmentFinished(segment.getSourceRows().height(), segment.getMapped().getColumn(CreateSegment.TARGET_COLUMN));
        e.getStats().incrementSegments(1);
    }

    public void targetsCommitted(Execution e, DeleteSegment segment) {
        e.getStats().incrementDeleted(segment.getMissingTargets().height());

        // call the logger before incrementing the segment count, so that start and end segment numbers match
        e.getLogger().deleteSegmentFinished(segment.getTargets().height(), segment.getMissingTargets().getColumn(DeleteSegment.TARGET_COLUMN));
        e.getStats().incrementSegments(1);
    }

    public void targetsCommitted(Execution e, CreateOrUpdateSegment segment) {
        BooleanSeries createdMask = segment.getMerged().getColumnAsBoolean(CreateOrUpdateSegment.TARGET_CREATED_COLUMN);

        Series<? extends Persistent> createdOrUpdated = segment.getMerged().getColumn(CreateOrUpdateSegment.TARGET_COLUMN);
        Series<? extends Persistent> created = createdOrUpdated.select(createdMask);
        Series<? extends Persistent> updated = createdOrUpdated.select(createdMask.not());

        e.getStats().incrementCreated(created.size());
        e.getStats().incrementUpdated(updated.size());

        // call the logger before incrementing the segment count, so that start and end segment numbers match
        e.getLogger().createOrUpdateSegmentFinished(segment.getSourceRows().height(), created, updated);
        e.getStats().incrementSegments(1);
    }

    public void targetsExtracted(Execution e, DeleteSegment segment) {
        e.getLogger().segmentStarted();
        e.getStats().incrementExtracted(segment.getTargets().height());
    }

    public void sourceKeysCollected(Execution e, SourceKeysSegment segment) {
        Set<?> keys = (Set<?>) e.getAttribute(SourceKeysTask.RESULT_KEY);
        Set<?> keysReported = keys != null ? keys : Collections.emptySet();

        // call the logger before incrementing the segment count, so that start and end segment numbers match
        e.getLogger().sourceKeysSegmentFinished(segment.getSourceRows().height(), keysReported);
        e.getStats().incrementSegments(1);
    }
}
