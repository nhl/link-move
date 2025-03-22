package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;

import java.math.BigDecimal;
import java.math.BigInteger;

public class IntegerConverter extends SingleTypeConverter<Integer> {

    private static final IntegerConverter CONVERTER = new IntegerConverter();

    public static IntegerConverter getConverter() {
        return CONVERTER;
    }

    @Override
    protected Class<Integer> targetType() {
        return Integer.class;
    }

    @Override
    protected Integer convertNotNull(Object value, int scale) {

        switch (value.getClass().getName()) {
            case "java.lang.Byte":
            case "java.lang.Short":
                return Integer.valueOf(value.toString());

            case "java.lang.Long":
                long longValue = (Long) value;
                if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
                    throw new LmRuntimeException("Value is too large for java.lang.Integer: " + longValue);
                }
                return (int) longValue; // safely truncate the value

            case "java.math.BigInteger":
                BigInteger bigInteger = (BigInteger) value;
                try {
                    return bigInteger.intValueExact();
                } catch (ArithmeticException e) {
                    throw new LmRuntimeException("Value is too large for java.lang.Integer: " + bigInteger);
                }

            case "java.math.BigDecimal":
                BigDecimal bigDecimal = (BigDecimal) value;
                try {
                    return bigDecimal.intValueExact();
                } catch (ArithmeticException e) {
                    throw new LmRuntimeException("Value can't be represented as java.lang.Integer: " + bigDecimal);
                }

            case "java.lang.String":
                String s = (String) value;
                return s.isEmpty() ? null : Integer.valueOf(s);

            default:
                throw new LmRuntimeException("Value can not be mapped to java.lang.Integer: " + value);
        }
    }
}
