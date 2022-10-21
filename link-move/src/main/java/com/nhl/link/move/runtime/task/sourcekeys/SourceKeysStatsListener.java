package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;

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
        e.getStats().incrementExtracted(segment.getSourceRows().height());
    }
}
