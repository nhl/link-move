package com.nhl.link.move.valueconverter;

import com.nhl.link.move.LmRuntimeException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

public class BigDecimalConverter extends SingleTypeConverter<BigDecimal> {

    @Override
    protected Class<BigDecimal> targetType() {
        return BigDecimal.class;
    }

    @Override
    protected BigDecimal convertAsCast(Object value, int scale) {
        BigDecimal converted = super.convertAsCast(value, scale);
        return scale >= 0 ? scale(converted, scale) : converted;
    }

    @Override
    protected BigDecimal convertNotNull(Object value, int scale) {
        BigDecimal converted = convertNoScale(value);
        return scale >= 0 ? scale(converted, scale) : converted;
    }

    private BigDecimal convertNoScale(Object value) {
        switch (value.getClass().getName()) {
            case "java.lang.String":
                String s = (String) value;
                return s.isEmpty() ? null : new BigDecimal(s);

            case "java.lang.Byte":
            case "java.lang.Short":
            case "java.lang.Integer":
            case "java.lang.Long":
                return BigDecimal.valueOf(Long.valueOf(value.toString()));

            case "java.lang.Float":
            case "java.lang.Double":
                return BigDecimal.valueOf(Double.valueOf(value.toString()));

            case "java.math.BigInteger":
                BigInteger bi = (BigInteger) value;
                return new BigDecimal(bi);

            default:
                throw new LmRuntimeException("Value can not be mapped to java.math.BigDecimal: " + value);
        }
    }

    /**
     * Returns a copy of {@code decimal} with {@code newScale} or throws an exception if scaling would result in precision loss.
     */
    private BigDecimal scale(BigDecimal decimal, int newScale) {

        // thankfully we don't have to deal with negative Java scales in JDBC
        if (newScale < 0) {
            throw new LmRuntimeException("Unexpected negative scale for JDBC attribute: " + newScale);
        }

        BigDecimal scaled;
        if (decimal.scale() == newScale) {
            scaled = decimal;
        } else if (decimal.scale() < newScale) {
            scaled = decimal.setScale(newScale, RoundingMode.UNNECESSARY); // safe to increase the scale
        } else {
            try {
                scaled = decimal.setScale(newScale, RoundingMode.UNNECESSARY);
            } catch (ArithmeticException e) {
                // happens if target scale is less than the initial scale and the scaled number does not have
                // enough trailing zeros to strip off without loss of precision
                throw new LmRuntimeException(String.format(
                        "Scaling of value of %s (scale: %d) to the scale of %d will result in precision loss",
                        decimal, decimal.scale(), newScale), e);
            }
        }
        return scaled;
    }
}
