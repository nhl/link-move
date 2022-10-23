package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.Execution;
import com.nhl.link.move.ExecutionStats;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsExtracted;

/**
 * A listener that collects task stats and stores them in the Execution's
 * {@link ExecutionStats} object.
 *
 * @since 1.3
 */
public class DeleteStatsListener {

    private static final DeleteStatsListener instance = new DeleteStatsListener();

    public static DeleteStatsListener instance() {
        return instance;
    }

    @AfterTargetsExtracted
    public void targetsExtracted(Execution e, DeleteSegment segment) {
        e.getLogger().segmentStarted();
        e.getStats().incrementExtracted(segment.getTargets().height());
    }

    @AfterTargetsCommitted
    public void targetsCommitted(Execution e, DeleteSegment segment) {
        e.getStats().incrementDeleted(segment.getMissingTargets().height());

        // call the logger before incrementing the segment count, so that start and end segment numbers match
        e.getLogger().deleteSegmentFinished(segment.getTargets().height(), segment.getMissingTargets().getColumn(DeleteSegment.TARGET_COLUMN));
        e.getStats().incrementSegments(1);
    }
}
