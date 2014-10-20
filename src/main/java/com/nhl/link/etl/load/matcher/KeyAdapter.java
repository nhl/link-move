package com.nhl.link.etl.load.matcher;

/**
 * Ensures that a given object can be used as a map key. The key may be wrapped
 * as needed if it is of a type that doesn't properly support "equals" and
 * "hashCode".
 * 
 * @since 1.1
 */
public interface KeyAdapter {

	Object toMapKey(Object rawKey);
	
	Object fromMapKey(Object mapKey);
}
