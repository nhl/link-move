package com.nhl.link.move.runtime.path;

import com.nhl.link.move.valueconverter.ValueConverterFactory;
import org.apache.cayenne.di.Inject;
import org.apache.cayenne.map.ObjEntity;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @since 1.4
 */
public class PathNormalizer implements IPathNormalizer {

    private ConcurrentMap<String, EntityPathNormalizer> pathCache;
    private ValueConverterFactory converterFactory;

    public PathNormalizer(@Inject ValueConverterFactory converterFactory) {
        pathCache = new ConcurrentHashMap<>();
        this.converterFactory = converterFactory;
    }

    @Override
    public EntityPathNormalizer normalizer(ObjEntity root) {
        Objects.requireNonNull(root, "Null root entity");
        return pathCache.computeIfAbsent(root.getName(), n -> createNormalizer(root));
    }

    private EntityPathNormalizer createNormalizer(final ObjEntity entity) {
        return new DefaultEntityPathNormalizer(entity, converterFactory);
    }
}
