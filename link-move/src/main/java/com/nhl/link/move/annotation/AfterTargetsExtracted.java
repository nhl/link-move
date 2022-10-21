package com.nhl.link.move.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation of a data segment transformation stage listener method that should be called after objects from the
 * target data store are extracted. This happens in {@link com.nhl.link.move.runtime.task.delete.DeleteTask} that
 * starts by extracting data from target.
 * 
 * @since 3.0
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface AfterTargetsExtracted {

}
