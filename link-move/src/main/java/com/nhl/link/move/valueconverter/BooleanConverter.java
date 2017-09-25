package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;

public class BooleanConverter extends SingleTypeConverter<Boolean> {

    private static final BooleanConverter CONVERTER = new BooleanConverter();

    public static BooleanConverter getConverter() {
        return CONVERTER;
    }

    @Override
    protected Class<Boolean> targetType() {
        return Boolean.class;
    }

    @Override
    protected Boolean convertNotNull(Object value, int scale) {

        switch (value.getClass().getName()) {
            case "java.lang.Byte":
            case "java.lang.Short":
            case "java.lang.Integer":
            case "java.lang.Long":
                Long number = Long.valueOf(value.toString());
                if (number == 0) {
                    return Boolean.FALSE;
                } else if (number == 1) {
                    return Boolean.TRUE;
                }
                // fall through

            default:
                throw new LmRuntimeException("Value can not be mapped to java.lang.Boolean: " + value);
        }
    }
}
