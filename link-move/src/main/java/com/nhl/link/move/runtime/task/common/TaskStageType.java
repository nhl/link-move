package com.nhl.link.move.runtime.task.common;

import java.lang.annotation.Annotation;

/**
 * A tag interface for enums that list stages of various tasks.
 *
 * @since 3.0.0
 */
public interface TaskStageType {

    /**
     * @deprecated the old style listeners were replaced with stage callbacks
     */
    @Deprecated(since = "3.0.0", forRemoval = true)
    Class<? extends Annotation> getLegacyAnnotation();
}
