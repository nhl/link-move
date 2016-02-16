package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;

import java.sql.Types;

/**
 * This normalizer upcasts byte, short and int values to long type.
 */
public class BigIntNormalizer extends JdbcNormalizer {

    public BigIntNormalizer() {
        super(Types.BIGINT);
    }

    @Override
    public Object normalize(Object value) {

        if (value == null) {
            return null;
        }

        switch (value.getClass().getName()) {
            case "java.lang.Long": {
                return value;
            }
            case "java.lang.Byte":
            case "java.lang.Short":
            case "java.lang.Integer": {
                return Long.valueOf(value.toString());
            }
            default: {
                throw new LmRuntimeException("Value can not be mapped to SQL " + getTypeName() + ": " + value);
            }
        }
    }
}
