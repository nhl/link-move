package com.nhl.link.move.runtime.task.common;

import java.lang.annotation.Annotation;

/**
 * @since 3.0
 */
public interface TaskStageType {
    Class<? extends Annotation> getLegacyAnnotation();
}
