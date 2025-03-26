package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;

import java.math.BigDecimal;

public class StringConverter extends SingleTypeConverter<String> {

    @Override
    protected Class<String> targetType() {
        return String.class;
    }

    @Override
    protected String convertNotNull(Object value, int scale) {

        switch (value.getClass().getName()) {
            case "java.lang.Byte":
            case "java.lang.Short":
            case "java.lang.Integer":
            case "java.lang.Long":
            case "java.math.BigInteger":
                return value.toString();
            case "java.lang.Float":
                Float f = (Float) value;
                if (f.isInfinite() || f.isNaN()) {
                    throw new LmRuntimeException("Cannot map a NaN/Infinity to String: " + value);
                }
                if (f != f.longValue()) {
                    throw new LmRuntimeException(
                            "Cannot map floating point number with non-zero fractional part to String: " + value);
                }
                return Long.toString(f.longValue());
            case "java.lang.Double":
                Double d = (Double) value;
                if (d.isInfinite() || d.isNaN()) {
                    throw new LmRuntimeException("Cannot map a NaN/Infinity to String: " + value);
                }
                if (d != d.longValue()) {
                    throw new LmRuntimeException(
                            "Cannot map floating point number with non-zero fractional part to String: " + value);
                }
                return Long.toString(d.longValue());
            case "java.math.BigDecimal":
                BigDecimal d = (BigDecimal) value;
                try {
                    return d.toBigIntegerExact().toString();
                } catch (ArithmeticException e) {
                    throw new LmRuntimeException(
                            "Cannot map java.math.BigDecimal with non-zero fractional part to String: " + value);
                }

            default:
                throw new LmRuntimeException("Value can not be mapped to String: " + value);
        }
    }
}
