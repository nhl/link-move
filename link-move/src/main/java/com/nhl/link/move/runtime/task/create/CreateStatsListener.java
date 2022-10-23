package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;
import com.nhl.link.move.annotation.AfterTargetsCommitted;

/**
 * @since 2.6
 */
public class CreateStatsListener {

    private static final CreateStatsListener instance = new CreateStatsListener();

    public static CreateStatsListener instance() {
        return instance;
    }

    @AfterSourceRowsExtracted
    public void sourceRowsExtracted(Execution e, CreateSegment segment) {
        e.getLogger().segmentStarted();
        e.getStats().incrementExtracted(segment.getSourceRows().height());
    }

    @AfterTargetsCommitted
    public void targetsCommitted(Execution e, CreateSegment segment) {
        e.getStats().incrementCreated(segment.getMapped().height());

        // call the logger before incrementing the batch count, so that start and end batch numbers match
        e.getLogger().createSegmentFinished(segment.getSourceRows().height(), segment.getMapped().getColumn(CreateSegment.TARGET_COLUMN));
        e.getStats().incrementSegments(1);
    }
}
