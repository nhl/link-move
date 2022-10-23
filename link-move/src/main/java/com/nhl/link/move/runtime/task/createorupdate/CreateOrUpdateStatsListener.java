package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.dflib.BooleanSeries;
import com.nhl.dflib.Series;
import com.nhl.link.move.Execution;
import com.nhl.link.move.ExecutionStats;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import org.apache.cayenne.Persistent;

/**
 * A listener that collects task stats and stores them in the Execution's {@link ExecutionStats} object.
 *
 * @since 1.3
 */
public class CreateOrUpdateStatsListener {

    private static final CreateOrUpdateStatsListener instance = new CreateOrUpdateStatsListener();

    public static CreateOrUpdateStatsListener instance() {
        return instance;
    }

    @AfterSourceRowsExtracted
    public void sourceRowsExtracted(Execution e, CreateOrUpdateSegment segment) {
        e.getLogger().segmentStarted();
        e.getStats().incrementExtracted(segment.getSourceRows().height());
    }

    @AfterTargetsCommitted
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
}
