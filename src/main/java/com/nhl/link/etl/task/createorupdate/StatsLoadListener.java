package com.nhl.link.etl.task.createorupdate;

import java.util.Map;

import com.nhl.link.etl.Execution;
import com.nhl.link.etl.LoadListener;

/**
 * A {@link LoadListener} that collects load stats and places them in the
 * execution. It is intentionally generics-free to work with any kind of root
 * entities.
 * 
 * @since 1.1
 */
@SuppressWarnings("rawtypes")
class StatsLoadListener implements LoadListener {

	private static final LoadListener instance = new StatsLoadListener();

	public static LoadListener instance() {
		return instance;
	}

	@Override
	public void targetCreated(Execution e, Map source, Object target) {
		e.incrementCreated(1);
	}

	@Override
	public void targetUpdated(Execution e, Map source, Object target) {
		e.incrementUpdated(1);
	}
}
