package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.map.DbAttribute;

public class BooleanNormalizer extends JdbcNormalizer<Boolean> {

    public BooleanNormalizer() {
        super(Boolean.class);
    }

    @Override
    public Boolean normalize(Object value, DbAttribute targetAttribute) {

        if (value == null) {
            return null;
        }

        switch (value.getClass().getName()) {
            case "java.lang.Byte":
            case "java.lang.Short":
            case "java.lang.Integer":
            case "java.lang.Long": {
                Long number = Long.valueOf(value.toString());
                if (number == 0) {
                    return Boolean.FALSE;
                } else if (number == 1) {
                    return Boolean.TRUE;
                }
                // fall through
            }
            default: {
                throw new LmRuntimeException("Value can not be mapped to " + getTypeName() + ": " + value);
            }
        }
    }
}
