package com.nhl.link.etl.keybuilder;

/**
 * Ensures that a given object can be used as a map key. The key may be wrapped
 * as needed if it is of a type that doesn't properly support "equals" and
 * "hashCode". 
 */
public interface KeyBuilder {

	Object toKey(Object rawKey);
}
