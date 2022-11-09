package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.link.move.annotation.AfterSourceKeysCollected;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;
import com.nhl.link.move.runtime.task.common.TaskStageType;

import java.lang.annotation.Annotation;

public enum SourceKeysStage implements TaskStageType {
    EXTRACT_SOURCE_ROWS(AfterSourceRowsExtracted.class),
    COLLECT_SOURCE_KEYS(AfterSourceKeysCollected.class);

    private final Class<? extends Annotation> legacyAnnotation;

    SourceKeysStage(Class<? extends Annotation> legacyAnnotation) {
        this.legacyAnnotation = legacyAnnotation;
    }

    @Override
    public Class<? extends Annotation> getLegacyAnnotation() {
        return legacyAnnotation;
    }
}
