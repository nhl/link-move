package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.map.DbAttribute;

import java.sql.Types;

public class IntegerNormalizer extends JdbcNormalizer {

    public IntegerNormalizer() {
        super(Types.INTEGER);
    }

    @Override
    public Object normalize(Object value) {
        return normalize(value, null);
    }

    @Override
    public Object normalize(Object value, DbAttribute targetAttribute) {

        if (value == null) {
            return null;
        }

        switch (value.getClass().getName()) {
            case "java.lang.Integer": {
                return value;
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
                throw new LmRuntimeException("Value can not be mapped to SQL " + getTypeName() + ": " + value);
            }
        }
    }
}
