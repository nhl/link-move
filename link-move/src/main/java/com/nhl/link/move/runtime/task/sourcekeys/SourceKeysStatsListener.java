package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterSourceKeysCollected;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;

import java.util.Set;

/**
 * @since 3.0
 */
public class SourceKeysStatsListener {

    private static final SourceKeysStatsListener instance = new SourceKeysStatsListener();

    public static SourceKeysStatsListener instance() {
        return instance;
    }

    @AfterSourceRowsExtracted
    public void sourceRowsExtracted(Execution e, SourceKeysSegment segment) {
        e.getLogger().batchStarted(e);
        e.getStats().incrementExtracted(segment.getSourceRows().height());
    }

    @AfterSourceKeysCollected
    public void sourceKeysCollected(Execution e, SourceKeysSegment segment) {
        Set<Object> keys = (Set<Object>) e.getAttribute(SourceKeysTask.RESULT_KEY);
        int keysExtracted = keys != null ? keys.size() : 0;

        // call the logger before incrementing the batch count, so that start and end batch numbers match
        e.getLogger().sourceKeysBatchFinished(e, segment.getSourceRows().height(), keysExtracted);
        e.getStats().incrementBatches(1);
    }
}
