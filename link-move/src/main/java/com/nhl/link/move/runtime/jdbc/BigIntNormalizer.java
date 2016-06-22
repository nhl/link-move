package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.map.DbAttribute;

/**
 * This normalizer upcasts byte, short and int values to long type.
 */
public class BigIntNormalizer extends JdbcNormalizer<Long> {

    public BigIntNormalizer() {
        super(Long.class);
    }

    @Override
    protected Long doNormalize(Object value, DbAttribute targetAttribute) {

        switch (value.getClass().getName()) {
            case "java.lang.String": {
                String s = (String) value;
                return s.isEmpty()? null : Long.valueOf(s);
            }
            case "java.lang.Byte":
            case "java.lang.Short":
            case "java.lang.Integer": {
                return Long.valueOf(value.toString());
            }
            default: {
                throw new LmRuntimeException("Value can not be mapped to " + getTypeName() + ": " + value);
            }
        }
    }
}
