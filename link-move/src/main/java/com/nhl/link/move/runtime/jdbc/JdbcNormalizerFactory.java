package com.nhl.link.move.runtime.jdbc;

import org.apache.cayenne.di.Inject;

import java.util.Map;

/**
 * @since 2.4
 */
public class JdbcNormalizerFactory {

    private Map<String, JdbcNormalizer> jdbcNormalizers;
    private JdbcNormalizer<?> doNothingNormalizer;

    public JdbcNormalizerFactory(@Inject Map<String, JdbcNormalizer> jdbcNormalizers) {
        this.jdbcNormalizers = jdbcNormalizers;
    }

    public <T> JdbcNormalizer<T> getNormalizer(String valueType) {
        return jdbcNormalizers.getOrDefault(valueType, doNothingNormalizer);
    }
}
