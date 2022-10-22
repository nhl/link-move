package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.dflib.Exp;
import com.nhl.dflib.Series;
import com.nhl.link.move.Execution;
import com.nhl.link.move.ExecutionStats;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;
import com.nhl.link.move.annotation.AfterTargetsCommitted;

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
    public void sourceRowsExtracted(Execution e, CreateOrUpdateSegment<?> segment) {
        e.getLogger().batchStarted(e);
        e.getStats().incrementExtracted(segment.getSourceRows().height());
    }

    @AfterTargetsCommitted
    public void targetsCommitted(Execution e, CreateOrUpdateSegment<?> segment) {
        Series<Boolean> wasCreated = segment.getMerged().getColumn(CreateOrUpdateSegment.TARGET_CREATED_COLUMN);

        int created = wasCreated.select(Exp.$bool("x")).size();
        int updated = wasCreated.size() - created;

        e.getStats().incrementCreated(created);
        e.getStats().incrementUpdated(updated);

        // call the logger before incrementing the batch count, so that start and end batch numbers match
        e.getLogger().createOrUpdateBatchFinished(e, segment.getSourceRows().height(), created, updated);
        e.getStats().incrementBatches(1);
    }
}
