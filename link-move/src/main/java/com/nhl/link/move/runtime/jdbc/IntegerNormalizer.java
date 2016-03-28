package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;

import java.sql.Types;

public class IntegerNormalizer extends JdbcNormalizer {

    public IntegerNormalizer() {
        super(Types.INTEGER);
    }

    @Override
    public Object normalize(Object value) {

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
