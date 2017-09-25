package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * This normalizer upcasts byte, short and int values to long type.
 */
public class LongConverter extends SingleTypeConverter<Long> {

    private static final LongConverter CONVERTER = new LongConverter();

    public static LongConverter getConverter() {
        return CONVERTER;
    }

    @Override
    protected Class<Long> targetType() {
        return Long.class;
    }

    @Override
    protected Long convertNotNull(Object value, int scale) {
        switch (value.getClass().getName()) {
            case "java.lang.Byte":
            case "java.lang.Short":
            case "java.lang.Integer":
                return Long.valueOf(value.toString());

            case "java.math.BigInteger":
                BigInteger bigInteger = (BigInteger) value;
                try {
                    return bigInteger.longValueExact();
                } catch (ArithmeticException e) {
                    throw new LmRuntimeException("Value is too large for java.lang.Long: " + bigInteger);
                }

            case "java.math.BigDecimal":
                BigDecimal bigDecimal = (BigDecimal) value;
                try {
                    return bigDecimal.longValueExact();
                } catch (ArithmeticException e) {
                    throw new LmRuntimeException("Value can't be represented as java.lang.Long: " + bigDecimal);
                }

            case "java.lang.String":
                String s = (String) value;
                return s.isEmpty() ? null : Long.valueOf(s);

            default:
                throw new LmRuntimeException("Value can not be mapped to java.lang.Long: " + value);
        }
    }
}
