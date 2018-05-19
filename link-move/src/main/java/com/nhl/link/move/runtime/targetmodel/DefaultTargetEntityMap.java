package com.nhl.link.move.runtime.targetmodel;

import org.apache.cayenne.map.ObjEntity;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @since 2.6
 */
public class DefaultTargetEntityMap implements TargetEntityMap {

    private ConcurrentMap<String, TargetEntity> entities;

    public DefaultTargetEntityMap() {
        entities = new ConcurrentHashMap<>();
    }

    @Override
    public TargetEntity get(ObjEntity root) {
        Objects.requireNonNull(root, "Null root entity");
        return entities.computeIfAbsent(root.getName(), n -> createEntity(root));
    }

    private TargetEntity createEntity(ObjEntity entity) {
        return new DefaultTargetEntity(this, entity);
    }
}
