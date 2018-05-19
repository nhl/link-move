package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.Execution;
import com.nhl.link.move.ExecutionStats;
import com.nhl.link.move.annotation.AfterTargetsMapped;

/**
 * @since 2.6
 */
public class CreateStatsListener {

    private static final CreateStatsListener instance = new CreateStatsListener();

    public static CreateStatsListener instance() {
        return instance;
    }

    @AfterTargetsMapped
    public void targetCreated(Execution e, CreateSegment<?> segment) {
        ExecutionStats stats = e.getStats();
        stats.incrementCreated(segment.getMapped().size());
    }
}
