package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.map.DbAttribute;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * This normalizer upcasts byte, short and int values to long type.
 */
public class LongNormalizer extends JdbcNormalizer<Long> {

    public LongNormalizer() {
        super(Long.class);
    }

    @Override
    protected Long doNormalize(Object value, DbAttribute targetAttribute) {
        switch (value.getClass().getName()) {
            case "java.lang.Byte":
            case "java.lang.Short":
            case "java.lang.Integer": {
                return Long.valueOf(value.toString());
            }
            case "java.math.BigInteger": {
                BigInteger bigInteger = (BigInteger) value;
                try {
                    return bigInteger.longValueExact();
                } catch (ArithmeticException e) {
                    throw new LmRuntimeException("Value is too large for java.lang.Long: " + bigInteger);
                }
            }
            case "java.math.BigDecimal": {
                BigDecimal bigDecimal = (BigDecimal) value;
                try {
                    return bigDecimal.longValueExact();
                } catch (ArithmeticException e) {
                    throw new LmRuntimeException("Value can't be represented as java.lang.Long: " + bigDecimal);
                }
            }
            case "java.lang.String": {
                String s = (String) value;
                return s.isEmpty()? null : Long.valueOf(s);
            }
            default: {
                throw new LmRuntimeException("Value can not be mapped to " + getTypeName() + ": " + value);
            }
        }
    }
}
