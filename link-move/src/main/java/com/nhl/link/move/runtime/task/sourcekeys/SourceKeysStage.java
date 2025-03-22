package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.link.move.runtime.task.common.TaskStageType;

/**
 * @since 3.0.0
 */
public enum SourceKeysStage implements TaskStageType {
    EXTRACT_SOURCE_ROWS,
    COLLECT_SOURCE_KEYS
}
