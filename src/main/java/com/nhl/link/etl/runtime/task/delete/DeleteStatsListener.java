package com.nhl.link.etl.runtime.task.delete;

import com.nhl.link.etl.Execution;
import com.nhl.link.etl.ExecutionStats;
import com.nhl.link.etl.annotation.AfterMissingTargetsFiltered;

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

	@AfterMissingTargetsFiltered
	public void targetCreated(Execution e, DeleteSegment<?> segment) {
		e.getStats().incrementDeleted(segment.getMissingTargets().size());
	}
}
