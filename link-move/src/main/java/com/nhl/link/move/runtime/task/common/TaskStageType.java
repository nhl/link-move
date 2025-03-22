package com.nhl.link.move.runtime.task.common;

import java.lang.annotation.Annotation;

/**
 * @since 3.0.0
 * @deprecated the old style listeners were replaced with stage callbacks
 */
@Deprecated(since = "3.0.0", forRemoval = true)
public interface TaskStageType {
    Class<? extends Annotation> getLegacyAnnotation();
}
