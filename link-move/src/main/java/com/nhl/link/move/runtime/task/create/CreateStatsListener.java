package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;
import com.nhl.link.move.annotation.AfterTargetsMapped;

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
        e.getStats().incrementExtracted(segment.getSourceRows().height());
    }

    @AfterTargetsMapped
    public void targetCreated(Execution e, CreateSegment<?> segment) {
        e.getStats().incrementCreated(segment.getMapped().height());
    }
}
