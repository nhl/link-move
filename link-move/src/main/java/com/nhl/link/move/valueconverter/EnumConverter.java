package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;

/**
 * @since 2.4
 */
public class EnumConverter<T extends Enum<T>> extends SingleTypeConverter<T> {

    private Class<T> enumType;

    public EnumConverter(Class<T> enumType) {
        this.enumType = enumType;
    }

    @Override
    protected Class<T> targetType() {
        return enumType;
    }

    @Override
    protected T convertNotNull(Object value, int scale) {

        // TODO: should we support enum ordinals?

        String enumString = value.toString();

        try {
            return Enum.valueOf(enumType, enumString);
        } catch (IllegalArgumentException e) {
            throw new LmRuntimeException("Invalid enum value: " + enumString);
        }
    }
}
