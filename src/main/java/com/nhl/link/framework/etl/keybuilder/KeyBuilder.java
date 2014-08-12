package com.nhl.link.framework.etl.keybuilder;

/**
 * Ensures that a given object can be used as a map key. The key may be wrapped
 * as needed if it is of a type that doesn't properly support "equals" and
 * "hashCode".
 * 
 * @since 6.14
 */
public interface KeyBuilder {

	Object toKey(Object rawKey);
}
