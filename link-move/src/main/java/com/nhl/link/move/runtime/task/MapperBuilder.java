package com.nhl.link.move.runtime.task;

import com.nhl.link.move.ClassNameResolver;
import com.nhl.link.move.mapper.KeyAdapter;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.mapper.MultiPathMapper;
import com.nhl.link.move.mapper.PathMapper;
import com.nhl.link.move.mapper.SafeMapKeyMapper;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.targetmodel.TargetAttribute;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import org.apache.cayenne.dba.TypesMapping;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.exp.parser.ASTDbPath;
import org.apache.cayenne.exp.property.Property;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.ObjAttribute;
import org.apache.cayenne.map.ObjEntity;
import org.apache.cayenne.map.ObjRelationship;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A helper dealing with {@link Mapper} assembly on behalf of {@link com.nhl.link.move.CreateOrUpdateBuilder} and
 * {@link com.nhl.link.move.DeleteBuilder}.
 *
 * @since 1.3
 */
public class MapperBuilder {

    private IKeyAdapterFactory keyAdapterFactory;
    private TargetEntity targetEntity;

    // TODO: now that we have TargetEntity, can we use it here instead of Cayenne ObjEntity
    private ObjEntity entity;
    private Set<String> paths;

    public MapperBuilder(ObjEntity entity, TargetEntity targetEntity, IKeyAdapterFactory keyAdapterFactory) {
        this.entity = entity;
        this.keyAdapterFactory = keyAdapterFactory;
        this.targetEntity = targetEntity;

        // Set will weed out simple duplicates , however we don't check for
        // invariants... so duplication is possible via db: vs obj: expressions
        this.paths = new HashSet<>();
    }

    public MapperBuilder matchBy(String... paths) {

        Objects.requireNonNull(paths, "Null 'paths'");

        for (String p : paths) {
            this.paths.add(p);
        }

        return this;
    }

    public MapperBuilder matchBy(Property<?>... paths) {

        Objects.requireNonNull(paths, "Null 'paths'");

        for (Property<?> p : paths) {
            this.paths.add(p.getName());
        }

        return this;
    }

    public MapperBuilder matchById() {

        int before = paths.size();
        entity.getDbEntity().getPrimaryKeys().forEach(pk -> this.paths.add(ASTDbPath.DB_PREFIX + pk.getName()));

        if (before == paths.size()) {
            throw new IllegalStateException("Target entity has no PKs defined: " + entity.getDbEntityName());
        }

        return this;
    }

    public Mapper build() {
        return createSafeKeyMapper(createMapper());
    }

    Mapper createSafeKeyMapper(Mapper unsafe) {
        KeyAdapter keyAdapter;

        if (paths.size() > 1) {
            // TODO: mapping keyMapAdapters by type doesn't take into account
            // composition and hierarchy of the keys ... need a different
            // approach. for now resorting to the hacks below

            keyAdapter = keyAdapterFactory.adapter(List.class);
        } else {

            Object attributeOrRelationship = ExpressionFactory.exp(paths.iterator().next()).evaluate(entity);

            Class<?> type;

            if (attributeOrRelationship instanceof ObjAttribute) {
                type = ((ObjAttribute) attributeOrRelationship).getJavaClass();
            } else if (attributeOrRelationship instanceof ObjRelationship) {
                type = ((ObjRelationship) attributeOrRelationship).getTargetEntity().getJavaClass();
            } else if (attributeOrRelationship instanceof DbAttribute) {
                DbAttribute dbAttribute = (DbAttribute) attributeOrRelationship;
                String typeName = TypesMapping.getJavaBySqlType(dbAttribute.getType());
                type = ClassNameResolver.typeForName(typeName);
            } else {
                type = null;
            }

            keyAdapter = keyAdapterFactory.adapter(type);
        }

        return new SafeMapKeyMapper(unsafe, keyAdapter);
    }

    Mapper createMapper() {
        Map<String, Mapper> mappers = createPathMappers();
        return mappers.size() > 1 ? new MultiPathMapper(mappers) : mappers.values().iterator().next();
    }

    Map<String, Mapper> createPathMappers() {

        if (paths.isEmpty()) {
            matchById();
        }

        // ensuring predictable attribute iteration order by alphabetically
        // ordering paths and using LinkedHashMap. Useful for unit test for one
        // thing.
        List<String> orderedPaths = new ArrayList<>(paths);
        Collections.sort(orderedPaths);

        Map<String, Mapper> mappers = new LinkedHashMap<>();
        for (String path : orderedPaths) {

            // TODO: should we skip invalid paths? Or are those for transient properties?
            String normalizedPath = targetEntity
                    .getAttribute(path)
                    .map(TargetAttribute::getNormalizedPath)
                    .orElse(path);
            mappers.put(normalizedPath, new PathMapper(normalizedPath));
        }

        return mappers;
    }
}
