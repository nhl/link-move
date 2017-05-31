package com.nhl.link.move.runtime.jdbc;

import org.apache.cayenne.map.DbAttribute;

/**
 * Used by LM matching runtime to convert extracted sources (on per-attribute basis) into a form that is compatible
 * with target entries. Each instance of this class should handle one JDBC type.
 *
 * @see {@link LongNormalizer}
 * @see {@link IntegerNormalizer}
 * @see {@link DecimalNormalizer}
 * @see {@link BooleanNormalizer}
 */
public abstract class JdbcNormalizer<T> {

    private final Class<T> type;

    public JdbcNormalizer(Class<T> javaType) {
        this.type = javaType;
    }

    /**
     * @return Default Java equivalent of the JDBC type that this normalizer works with.
     * @see java.sql.Types
     */
    public Class<T> getType() {
        return type;
    }

    public String getTypeName() {
        return type.getName();
    }

    /**
     * @since 1.7
     */
    @SuppressWarnings("unchecked")
    public T normalize(Object value, DbAttribute targetAttribute) {

        T result;
        if (value == null) {
            result = null;
        } else if (type.isAssignableFrom(value.getClass())) {
            result = (T) value;
        } else {
            result = doNormalize(value, targetAttribute);
        }

        return postNormalize(result, targetAttribute);
    }

    protected abstract T doNormalize(Object value, DbAttribute targetAttribute);

    /**
     * Override this method to do post-processing of the normalized value
     * (e.g. additional scaling of a decimal)
     *
     * @see DecimalNormalizer
     */
    protected T postNormalize(T normalized, DbAttribute targetAttribute) {
        // by default just return the value
        return normalized;
    }
}
