package com.nhl.link.etl.runtime.task.createorupdate;

import com.nhl.link.etl.CreateOrUpdateSegment;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.annotation.AfterTargetMerged;
import com.nhl.link.etl.stats.ExecutionStats;

/**
 * A listener that collects load stats and places them in the execution. It is
 * intentionally generics-free to work with any kind of root entities.
 * 
 * @since 1.1
 */
public class StatsLoadListener {

	private static final StatsLoadListener instance = new StatsLoadListener();

	public static StatsLoadListener instance() {
		return instance;
	}

	@AfterTargetMerged
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
