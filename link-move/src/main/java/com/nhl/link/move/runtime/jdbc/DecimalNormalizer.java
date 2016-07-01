package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.map.DbAttribute;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

public class DecimalNormalizer extends JdbcNormalizer<BigDecimal> {

    public DecimalNormalizer() {
        super(BigDecimal.class);
    }

    @Override
    protected BigDecimal doNormalize(Object value, DbAttribute targetAttribute) {

        BigDecimal normalized;
        switch (value.getClass().getName()) {
            case "java.lang.String": {
                String s = (String) value;
                normalized = s.isEmpty()? null : new BigDecimal(s);
                break;
            }
            case "java.lang.Byte":
            case "java.lang.Short":
            case "java.lang.Integer":
            case "java.lang.Long": {
                normalized = BigDecimal.valueOf(Long.valueOf(value.toString()));
                break;
            }
            case "java.lang.Float":
            case "java.lang.Double": {
                normalized = BigDecimal.valueOf(Double.valueOf(value.toString()));
                break;
            }
            case "java.math.BigInteger": {
                BigInteger bi = (BigInteger) value;
                normalized = new BigDecimal(bi);
                break;
            }
            default: {
                throw new LmRuntimeException("Value can not be mapped to " + getTypeName() + ": " + value);
            }
        }

        return normalized;
    }

    @Override
    protected BigDecimal postNormalize(BigDecimal normalized, DbAttribute targetAttribute) {
        if (normalized == null) {
            return null;
        } else if (targetAttribute == null) {
            return normalized;
        } else {
            return scale(normalized, targetAttribute.getScale());
        }
    }

    /**
     * Returns a copy of {@code decimal} with {@code newScale} or
     * throws an exception if scaling would result in precision loss.
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
