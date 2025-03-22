package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.runtime.task.common.TaskStageType;

/**
 * @since 3.0.0
 */
public enum CreateOrUpdateStage implements TaskStageType {
    EXTRACT_SOURCE_ROWS,
    CONVERT_SOURCE_ROWS,
    MAP_SOURCE,
    MATCH_TARGET,
    MAP_TARGET,
    RESOLVE_FK_VALUES,
    MERGE_TARGET,
    COMMIT_TARGET
}
