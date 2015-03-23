package com.nhl.link.etl.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation of a data segment transformation stage listener method that should
 * be called after objects are pulled from the target data store for a set of
 * source keys.
 * 
 * @since 1.3
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface AfterTargetMatched {

}
