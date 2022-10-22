package com.nhl.link.move.writer;

/**
 * An object that can set a single property of a given kind of object. Typical
 * implementations can include writing attribute object properties, ids, or
 * relationships.
 *
 * @since 1.4
 */
@FunctionalInterface
public interface TargetPropertyWriter {

    /**
     * Sets a value of a property corresponding to this writer of a target DataObject.
     */
    void write(Object target, Object value);

    /**
     * Default implementation returns false.
     *
     * @return true if the state of the object will be affected by the update, false otherwise.
     */
    default boolean willWrite(Object target, Object value) {
        return false;
    }
}
