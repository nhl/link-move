package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.ClassNameResolver;
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
        this.doNothingNormalizer = (v, a) -> v;
    }

    public <T> JdbcNormalizer<T> getNormalizer(String valueType) {
        return jdbcNormalizers.computeIfAbsent(valueType, this::createNormalizer);
    }

    private <T> JdbcNormalizer<T> createNormalizer(String valueType) {
        Class<?> type = ClassNameResolver.typeForName(valueType);
        return (type.isEnum())
                ? new EnumNormalizer<>((Class<? extends Enum>) type)
                : (JdbcNormalizer<T>) doNothingNormalizer;
    }

}
