package com.nhl.link.move.writer;

import org.apache.cayenne.DataObject;

/**
 * An object that can set a single property of a given kind of object. Typical
 * implementations can include writing attribute object properties, ids, or
 * relationships.
 * 
 * @since 1.4
 */
public interface TargetPropertyWriter {

	/**
	 * Sets a value of a property corresponding to this writer of a target
	 * DataObject.
	 * 
	 * @return true if the state of the object was affected, false otherwise.
	 */
	boolean write(DataObject target, Object value);
}
