package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.map.DbAttribute;

public class IntegerNormalizer extends JdbcNormalizer<Integer> {

    public IntegerNormalizer() {
        super(Integer.class);
    }

    @Override
    public Integer normalize(Object value, DbAttribute targetAttribute) {

        if (value == null) {
            return null;
        }

        switch (value.getClass().getName()) {
            case "java.lang.Integer": {
                return (Integer) value;
            }
            case "java.lang.Long": {
                return ((Long) value).intValue(); // truncating the value
            }
            case "java.lang.String": {
                String s = (String) value;
                return s.isEmpty()? null : Integer.valueOf(s);
            }
            case "java.lang.Byte":
            case "java.lang.Short": {
                return Integer.valueOf(value.toString());
            }
            default: {
                throw new LmRuntimeException("Value can not be mapped to " + getTypeName() + ": " + value);
            }
        }
    }
}
