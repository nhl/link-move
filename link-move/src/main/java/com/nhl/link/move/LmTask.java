package com.nhl.link.move;

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
	Execution run();

	/**
	 * Executes the task with a map of parameters returning {@link Execution}
	 * object that can be used by the caller to analyze the results. Currently
	 * all task implementations are synchronous, so this method returns only on
	 * task completion.
	 * 
	 * @since 1.3
	 */
	Execution run(Map<String, ?> params);

	/**
	 * Executes the task with a map of parameters returning {@link Execution}
	 * object that can be used by the caller to analyze the results. Currently
	 * all task implementations are synchronous, so this method returns only on
	 * task completion.
	 */
	Execution run(SyncToken token);

	/**
	 * Executes the task with a map of parameters returning {@link Execution}
	 * object that can be used by the caller to analyze the results. Currently
	 * all task implementations are synchronous, so this method returns only on
	 * task completion.
	 * 
	 * @since 1.3
	 */
	Execution run(SyncToken token, Map<String, ?> params);

	/**
	 * Executes the task returning {@link Execution} object that can be used by
	 * the caller to analyze the results. Currently all task implementations are
	 * synchronous, so this method returns only on task completion.
	 *
	 * @since 2.3
	 */
	Execution dryRun();

	/**
	 * Executes the task with a map of parameters returning {@link Execution}
	 * object that can be used by the caller to analyze the results. Currently
	 * all task implementations are synchronous, so this method returns only on
	 * task completion.
	 *
	 * @since 2.3
	 */
	Execution dryRun(Map<String, ?> params);

	/**
	 * Executes the task with a map of parameters returning {@link Execution}
	 * object that can be used by the caller to analyze the results. Currently
	 * all task implementations are synchronous, so this method returns only on
	 * task completion.
	 *
	 * @since 2.3
	 */
	Execution dryRun(SyncToken token);

	/**
	 * Executes the task with a map of parameters returning {@link Execution}
	 * object that can be used by the caller to analyze the results. Currently
	 * all task implementations are synchronous, so this method returns only on
	 * task completion.
	 *
	 * @since 2.3
	 */
	Execution dryRun(SyncToken token, Map<String, ?> params);

}
