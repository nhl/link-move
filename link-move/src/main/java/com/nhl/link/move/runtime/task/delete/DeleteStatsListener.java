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
    public void targetsExtracted(Execution e, DeleteSegment<?> segment) {
        e.getLogger().batchStarted(e);
        // TODO: not reporting Execution.extracted metric because it is set by the SourceKeysTask..
        //  probably need to report both target and source extractors
    }

    @AfterTargetsCommitted
    public void targetsCommitted(Execution e, DeleteSegment<?> segment) {
        e.getStats().incrementDeleted(segment.getMissingTargets().height());
        e.getLogger().deleteBatchFinished(e, segment.getTargets().height(), segment.getMissingTargets().height());
    }
}
