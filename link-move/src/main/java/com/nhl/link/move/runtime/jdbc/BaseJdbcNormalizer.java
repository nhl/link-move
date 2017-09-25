package com.nhl.link.move.runtime.jdbc;

import org.apache.cayenne.map.DbAttribute;

/**
 * Used by LM matching runtime to convert extracted sources (on per-attribute basis) into a form that is compatible
 * with target entries. Each instance of this class should handle one JDBC type.
 */
public abstract class BaseJdbcNormalizer<T> implements JdbcNormalizer {

    /**
     * @since 1.7
     */
    @SuppressWarnings("unchecked")
    @Override
    public T normalize(Object value, DbAttribute targetAttribute) {

        T result;
        if (value == null) {
            result = null;
        } else {
            result = doNormalize(value, targetAttribute);
        }

        return postNormalize(result, targetAttribute);
    }

    protected abstract T doNormalize(Object value, DbAttribute targetAttribute);

    /**
     * Override this method to do post-processing of the normalized value (e.g. additional scaling of a decimal).
     *
     * @see BigDecimalNormalizer
     */
    protected T postNormalize(T normalized, DbAttribute targetAttribute) {
        // by default just return the value
        return normalized;
    }
}
