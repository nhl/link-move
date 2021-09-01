package com.nhl.link.move.annotation;

import java.lang.annotation.*;

/**
 * Annotation of a data segment transformation stage listener method that is called after the source rows are extracted,
 * but before any jon-related conversions.
 * 
 * @since 2.16
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface AfterSourceRowsExtracted {

}
