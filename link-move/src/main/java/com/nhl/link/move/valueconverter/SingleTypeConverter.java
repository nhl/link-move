package com.nhl.link.move.valueconverter;

/**
 * A base superclass into of ValueConverters. Each subclass will produce a single target Java type.
 *
 * @since 2.4
 */
public abstract class SingleTypeConverter<T> implements ValueConverter {

    @Override
    public T convert(Object value, int scale) {
        // ignoring scale...

        if (value == null) {
            return null;
        } else if (targetType().isAssignableFrom(value.getClass())) {
            return convertAsCast(value, scale);
        } else {
            return convertNotNull(value, scale);
        }
    }

    protected abstract Class<T> targetType();

    protected T convertAsCast(Object value, int scale) {
        return (T) value;
    }

    protected abstract T convertNotNull(Object value, int scale);
}
