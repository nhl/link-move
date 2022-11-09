package com.nhl.link.move.annotation;

import java.lang.annotation.*;

/**
 * Annotation of a data segment transformation stage listener method that should be called after FK values
 * are resolved to objects.
 *
 * @since 2.12
 *
 * @deprecated use lambda-based callbacks instead, @see {@link com.nhl.link.move.runtime.task.BaseTaskBuilder#stage}
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Deprecated(since = "3.0")
public @interface AfterFksResolved {

}
