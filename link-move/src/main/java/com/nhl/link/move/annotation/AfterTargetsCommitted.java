package com.nhl.link.move.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation of a data segment transformation stage listener method that should
 * be called after objects are committed to the target date store.
 *
 * @since 2.0
 *
 * @deprecated use lambda-based callbacks instead, @see {@link com.nhl.link.move.runtime.task.BaseTaskBuilder#stage}
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Deprecated(since = "3.0")
public @interface AfterTargetsCommitted {
}
