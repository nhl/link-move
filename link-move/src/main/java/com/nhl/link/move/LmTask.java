package com.nhl.link.move;

import java.util.Collections;
import java.util.Map;

/**
 * An abstraction of a runnable data transfer task.
 */
public interface LmTask {

	/**
	 * Executes the task returning {@link Execution} object that can be used by
	 * the caller to analyze the results. Currently all task implementations are
	 * synchronous, so this method returns only on task completion.
	 * 
	 * @since 1.1
	 */
	default Execution run() {
		return run(Collections.emptyMap());
	}

	/**
	 * Executes the task with a map of parameters returning {@link Execution}
	 * object that can be used by the caller to analyze the results. Currently
	 * all task implementations are synchronous, so this method returns only on
	 * task completion.
	 * 
	 * @since 1.3
	 */
	default Execution run(Map<String, ?> params) {
		return run(null, params);
	}

	/**
	 * Executes the task with a map of parameters returning {@link Execution}
	 * object that can be used by the caller to analyze the results. Currently
	 * all task implementations are synchronous, so this method returns only on
	 * task completion.
	 */
	default Execution run(SyncToken token) {
		return run(token, Collections.emptyMap());
	}

	/**
	 * Executes the task with a map of parameters returning {@link Execution}
	 * object that can be used by the caller to analyze the results. Currently
	 * all task implementations are synchronous, so this method returns only on
	 * task completion.
	 * 
	 * @since 1.3
	 */
	Execution run(SyncToken token, Map<String, ?> params);

}
