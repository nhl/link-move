package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.annotation.AfterMissingTargetsFiltered;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsExtracted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.runtime.task.common.TaskStageType;

import java.lang.annotation.Annotation;

public enum DeleteStage implements TaskStageType {
    EXTRACT_TARGET(AfterTargetsExtracted.class),
    MAP_TARGET(AfterTargetsMapped.class),
    FILTER_MISSING_TARGETS(AfterMissingTargetsFiltered.class),
    COMMIT_TARGET(AfterTargetsCommitted.class);

    private final Class<? extends Annotation> legacyAnnotation;

    DeleteStage(Class<? extends Annotation> legacyAnnotation) {
        this.legacyAnnotation = legacyAnnotation;
    }

    @Override
    public Class<? extends Annotation> getLegacyAnnotation() {
        return legacyAnnotation;
    }
}
