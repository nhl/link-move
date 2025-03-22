package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.runtime.task.common.TaskStageType;

/**
 * @since 3.0.0
 */
public enum DeleteStage implements TaskStageType {
    EXTRACT_TARGET,
    MAP_TARGET,
    FILTER_MISSING_TARGETS,
    COMMIT_TARGET
}
