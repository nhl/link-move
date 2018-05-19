package com.nhl.link.move.runtime.task;

import com.nhl.link.move.Execution;

/**
 * A wrapper of an annotated listener method for a single execution stage of a task.
 * 
 * @since 1.3
 */
public interface StageListener {

	void afterStageFinished(Execution exec, Object segment);
}
