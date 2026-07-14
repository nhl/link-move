package com.nhl.link.move.runtime.targetmodel;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.dba.TypesMapping;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.exp.parser.ASTDbPath;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbEntity;
import org.apache.cayenne.map.DbJoin;
import org.apache.cayenne.map.DbRelationship;
import org.apache.cayenne.map.ObjAttribute;
import org.apache.cayenne.map.ObjEntity;
import org.apache.cayenne.map.PathComponent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @since 2.6
 */
public class DefaultTargetEntity implements TargetEntity {

    private TargetEntityMap entityMap;
    private ObjEntity entity;
    // may contain more than one entry for a single logical attribute (for normalized and un-normalized paths).
    private Map<String, Optional<TargetAttribute>> attributes;

    public DefaultTargetEntity(TargetEntityMap entityMap, ObjEntity entity) {
        this.entityMap = entityMap;
        this.entity = entity;
        this.attributes = new ConcurrentHashMap<>();
    }

    @Override
    public String getName() {
        return entity.getName();
    }

    @Override
    public Optional<TargetAttribute> getAttribute(String path) {
        // TODO: ensure we don't create duplicate attributes for path invariants..
        // while this doesn't seem to harm anything, it just feels dirty.. And sooner or later we'd allow to iterate
        // over entity attributes and then it becomes a problem
        return attributes.computeIfAbsent(path, this::createAttribute);
    }

    protected Optional<TargetAttribute> createAttribute(String path) {

        if (!hasAttributeOrRelationship(path)) {
            return Optional.empty();
        }

        // this "normalizes" the path
        Expression dbExp = entity.translateToDbPath(ExpressionFactory.exp(path));

        List<PathComponent<DbAttribute, DbRelationship>> components = new ArrayList<>(2);
        entity.getDbEntity().resolvePath(dbExp, Collections.emptyMap()).forEach(components::add);

        if (components.isEmpty()) {
            throw new LmRuntimeException("Null path. Entity: " + entity.getName());
        }

        PathComponent<DbAttribute, DbRelationship> c = components.get(components.size() - 1);

        TargetAttribute attribute = c.getAttribute() != null
                ? createAttributeForAttribute(dbExp, components)
                : createAttributeForRelationship(dbExp, components);

        return Optional.of(attribute);
    }

    private boolean hasAttributeOrRelationship(String path) {
        if (path.startsWith(ASTDbPath.DB_PREFIX)) {
            String dbPath = path.substring(ASTDbPath.DB_PREFIX.length());
            DbEntity dbEntity = entity.getDbEntity();
            return dbEntity.getAttribute(dbPath) != null || dbEntity.getRelationship(dbPath) != null;
        } else {
            return entity.getAttribute(path) != null || entity.getRelationship(path) != null;
        }
    }

    protected TargetAttribute createAttributeForAttribute(Expression dbExp, List<PathComponent<DbAttribute, DbRelationship>> components) {

        // there can be more than one join for an attribute, only some being straight FKs
        // TODO: still kinda undeterministic.

        DbAttribute attribute = components.get(components.size() - 1).getAttribute();

        Optional<ForeignKey> fk = findAllRelationships(attribute)
                .stream()
                .map(r -> createFk(r, attribute))
                .filter(Optional::isPresent)
                .findFirst()
                .orElse(Optional.empty());

        return new TargetAttribute(
                this,
                dbExp.toString(),
                attribute.getScale(),
                javaType(attribute),
                fk);
    }

    protected TargetAttribute createAttributeForRelationship(Expression dbExp, List<PathComponent<DbAttribute, DbRelationship>> components) {

        // TODO: support flattened relationships. We already support flattened attributes
        //  (see "createAttributeForAttribute" above). Do they make sense in the context of an ETL though?

        if (components.size() > 1) {
            throw new LmRuntimeException("Flattened relationships not supported. Path: " + dbExp);
        }

        DbRelationship relationship = components.get(components.size() - 1).getRelationship();

        List<DbJoin> joins = relationship.getJoins();

        if (joins.size() > 1) {
            // A multi-column FK has no single-column source representation when mapped via the bare
            // relationship name. Map its FK columns explicitly instead (e.g. via their "db:" paths),
            // which is handled by "createAttributeForAttribute" above.
            throw new LmRuntimeException("Can't map a multi-column FK via a relationship name."
                    + " Map its FK columns instead. Relationship: "
                    + relationship.getSourceEntityName()
                    + "."
                    + relationship.getName());
        }

        DbJoin firstJoin = joins.get(0);

        return new TargetAttribute(
                this,
                ASTDbPath.DB_PREFIX + firstJoin.getSourceName(),
                firstJoin.getSource().getScale(),
                javaType(firstJoin.getSource()),
                createFk(relationship, firstJoin.getSource()));
    }

    private Optional<ForeignKey> createFk(DbRelationship relationship, DbAttribute attribute) {

        // What we decide to be a FK or not must roughly match the algorithm in
        // TargetPropertyWriterService.createWriterFactory(..), or we won't be able to match the data columns with
        // object properties

        // TODO: there's no full correspondence, as TargetPropertyWriterService checks for object properties

        DbEntity targetDbEntity = relationship.getTargetEntity();
        List<DbJoin> joins = relationship.getJoins();

        // only the FK side of a to-one (i.e. the entity that actually holds the FK columns)
        if (relationship.isSourceIndependentFromTargetChange()) {
            return Optional.empty();
        }

        // A multi-column FK is detected once, on the first join's source column, so that a single
        // TargetAttribute owns the whole FK (it knows all of its source columns). The other component
        // columns stay plain. The resolved object is later placed in a separate, relationship-named
        // column (see ForeignKey.getResultColumn()), not in any of these FK columns.
        if (!attribute.getName().equals(joins.get(0).getSourceName())) {
            return Optional.empty();
        }

        // The relationship is resolved by matching FK values against the target's PK, so the FK
        // column count must line up with the target PK column count.
        if (joins.size() != targetDbEntity.getPrimaryKeys().size()) {
            return Optional.empty();
        }

        Collection<ObjEntity> targetEntities = targetDbEntity.getDataMap().getMappedEntities(targetDbEntity);

        switch (targetEntities.size()) {
            case 0:
                return Optional.empty();
            case 1:
                List<String> sourceColumns = new ArrayList<>(joins.size());
                List<String> targetPaths = new ArrayList<>(joins.size());
                for (DbJoin join : joins) {
                    sourceColumns.add(ASTDbPath.DB_PREFIX + join.getSourceName());
                    targetPaths.add(ASTDbPath.DB_PREFIX + join.getTargetName());
                }
                // the resolved object lands in a column named after the relationship, matching the
                // writer registration in TargetPropertyWriterFactory.initWriter(ToOneProperty)
                String resultColumn = ASTDbPath.DB_PREFIX + relationship.getName();
                return Optional.of(new ForeignKey(entityMap, targetEntities.iterator().next(), sourceColumns, targetPaths, resultColumn));
            default:
                throw new LmRuntimeException("Unsupported FK: DbEntity "
                        + targetDbEntity.getName()
                        + " maps to more than one ObjEntity");
        }
    }

    private String javaType(DbAttribute attribute) {
        ObjAttribute objAttribute = entity.getAttributeForDbAttribute(attribute);
        return (objAttribute != null)
                ? objAttribute.getType()
                : TypesMapping.getJavaBySqlType(attribute.getType());
    }

    private Collection<DbRelationship> findAllRelationships(DbAttribute attribute) {

        Collection<DbRelationship> candidates = new ArrayList<>(2);
        String attributeName = attribute.getName();

        for (DbRelationship relationship : attribute.getEntity().getRelationships()) {
            for (DbJoin join : relationship.getJoins()) {
                if (attributeName.equals(join.getSourceName())) {
                    candidates.add(relationship);
                    break;
                }
            }
        }

        return candidates;
    }
}
