package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.map.DbAttribute;

public class BooleanNormalizer extends JdbcNormalizer<Boolean> {

    private static final BooleanNormalizer normalizer = new BooleanNormalizer();

    public static BooleanNormalizer getNormalizer() {
        return normalizer;
    }

    public BooleanNormalizer() {
        super(Boolean.class);
    }

    @Override
    protected Boolean doNormalize(Object value, DbAttribute targetAttribute) {

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
