package com.nhl.link.etl.runtime.task.createorupdate;

import com.nhl.link.etl.CreateOrUpdateSegment;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.annotation.AfterTargetsMerged;
import com.nhl.link.etl.stats.ExecutionStats;

/**
 * A listener that collects task stats and stores them in the Execution's
 * {@link ExecutionStats} object.
 * 
 * @since 1.3
 */
public class CreateOrUpdateStatsListener {

	private static final CreateOrUpdateStatsListener instance = new CreateOrUpdateStatsListener();

	public static CreateOrUpdateStatsListener instance() {
		return instance;
	}

	@AfterTargetsMerged
	public void targetCreated(Execution e, CreateOrUpdateSegment<?> segment) {

		ExecutionStats stats = e.getStats();

		for (CreateOrUpdateTuple<?> t : segment.getMerged()) {
			if (t.isCreated()) {
				stats.incrementCreated(1);
			} else {
				stats.incrementUpdated(1);
			}

		}
	}
}
