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
    public void sourceRowsExtracted(Execution e, CreateSegment<?> segment) {
        e.getLogger().batchStarted(e);
        e.getStats().incrementExtracted(segment.getSourceRows().height());
    }

    @AfterTargetsCommitted
    public void targetsCommitted(Execution e, CreateSegment<?> segment) {
        e.getStats().incrementCreated(segment.getMapped().height());
        e.getLogger().createBatchFinished(e, segment.getSourceRows().height(), segment.getMapped().height());
    }
}
