package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.annotation.AfterFksResolved;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.runtime.task.common.TaskStageType;

import java.lang.annotation.Annotation;

public enum CreateStage implements TaskStageType {
    EXTRACT_SOURCE_ROWS(AfterSourceRowsExtracted.class),
    CONVERT_SOURCE_ROWS(AfterSourceRowsConverted.class),
    MAP_TARGET(AfterTargetsMapped .class),
    RESOLVE_FK_VALUES(AfterFksResolved.class),
    MERGE_TARGET(AfterTargetsMerged.class),
    COMMIT_TARGET(AfterTargetsCommitted.class);


    private final Class<? extends Annotation> legacyAnnotation;

    CreateStage(Class<? extends Annotation> legacyAnnotation) {
        this.legacyAnnotation = legacyAnnotation;
    }

    @Override
    public Class<? extends Annotation> getLegacyAnnotation() {
        return legacyAnnotation;
    }
}
