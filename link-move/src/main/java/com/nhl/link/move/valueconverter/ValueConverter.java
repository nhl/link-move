package com.nhl.link.move.valueconverter;

/**
 * Converts data values coming from the ETL source to the preset type supported by this converter.
 *
 * @since 2.4
 */
public interface ValueConverter {

    /**
     * Value conversion method. Uses optional scale value from the target DB attribute.
     */
    Object convert(Object value, int scale);
}
