package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.runtime.task.common.TaskStageType;

/**
 * @since 3.0.0
 */
public enum CreateStage implements TaskStageType {
    EXTRACT_SOURCE_ROWS,
    CONVERT_SOURCE_ROWS,
    MAP_TARGET,
    RESOLVE_FK_VALUES,
    MERGE_TARGET,
    COMMIT_TARGET
}
