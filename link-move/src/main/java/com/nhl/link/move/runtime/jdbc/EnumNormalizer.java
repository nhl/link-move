package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.map.DbAttribute;

/**
 * @since 2.4
 */
public class EnumNormalizer<T extends Enum<T>> extends BaseJdbcNormalizer<T> {

    public EnumNormalizer(Class<T> javaType) {
        super(javaType);
    }

    @Override
    protected T doNormalize(Object value, DbAttribute targetAttribute) {

        // TODO: should we support enum ordinals?

        String enumString = value.toString();

        try {
            return Enum.valueOf(getType(), enumString);
        } catch (IllegalArgumentException e) {
            throw new LmRuntimeException("Invalid enum value: " + enumString);
        }
    }
}
