package com.nhl.link.move.valueconverter;

import com.nhl.link.move.ClassNameResolver;
import org.apache.cayenne.di.Inject;

import java.util.Map;

/**
 * @since 2.4
 */
public class ValueConverterFactory {

    private final Map<String, ValueConverter> converters;
    private final ValueConverter noConverter;

    public ValueConverterFactory(@Inject Map<String, ValueConverter> converters) {
        this.converters = converters;
        this.noConverter = (v, a) -> v;
    }

    public ValueConverter getConverter(String valueType) {
        return converters.computeIfAbsent(valueType, this::createConverter);
    }

    private ValueConverter createConverter(String valueType) {
        Class<?> type = ClassNameResolver.typeForName(valueType);
        return (type.isEnum())
                ? new EnumConverter<>((Class<? extends Enum>) type)
                : noConverter;
    }

}
