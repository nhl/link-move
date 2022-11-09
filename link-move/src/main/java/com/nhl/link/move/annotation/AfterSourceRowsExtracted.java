package com.nhl.link.move.annotation;

import java.lang.annotation.*;

/**
 * Annotation of a data segment transformation stage listener method that is called after the source rows are extracted,
 * but before any jon-related conversions.
 * 
 * @since 2.16
 *
 * @deprecated use lambda-based callbacks instead, @see {@link com.nhl.link.move.runtime.task.BaseTaskBuilder#stage}
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Deprecated(since = "3.0")
public @interface AfterSourceRowsExtracted {

}
