package com.nhl.link.move.valueconverter;

/**
 * Converts data values coming from the ETL source to the preset type supported by this converter.
 *
 * @since 2.4
 */
public interface ValueConverter {

    default Object convert(Object value) {
        return convert(value, -1);
    }

    Object convert(Object value, int scale);
}
