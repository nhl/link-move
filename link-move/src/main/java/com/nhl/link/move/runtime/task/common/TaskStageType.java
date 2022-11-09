package com.nhl.link.move.runtime.task.common;

import java.lang.annotation.Annotation;

public interface TaskStageType {
    Class<? extends Annotation> getLegacyAnnotation();
}
