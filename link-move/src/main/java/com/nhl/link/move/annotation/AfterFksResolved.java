package com.nhl.link.move.annotation;

import java.lang.annotation.*;

/**
 * Annotation of a data segment transformation stage listener method that should be called after FK values
 * are resolved to objects.
 *
 * @since 2.12
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface AfterFksResolved {

}
