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
        // over entitye attributes and then it becomes a problem
        return attributes.computeIfAbsent(path, this::createAttribute);
    }

    protected Optional<TargetAttribute> createAttribute(String path) {

        if (!hasAttributeOrRelationship(path)) {
            return Optional.empty();
        }

        // this "normalizes" the path
        Expression dbExp = entity.translateToDbPath(ExpressionFactory.exp(path));

        List<PathComponent<DbAttribute, DbRelationship>> components = new ArrayList<>(2);
        for (PathComponent<DbAttribute, DbRelationship> c : entity.getDbEntity()
                .resolvePath(dbExp, Collections.emptyMap())) {
            components.add(c);
        }

        if (components.size() == 0) {
            throw new LmRuntimeException("Null path. Entity: " + entity.getName());
        }

        if (components.size() > 1) {
            throw new LmRuntimeException("Nested paths not supported. Path: " + dbExp);
        }

        PathComponent<DbAttribute, DbRelationship> c = components.get(0);

        TargetAttribute attribute = c.getAttribute() != null
                ? createAttribute(c.getAttribute())
                : createAttribute(c.getRelationship());

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

    protected TargetAttribute createAttribute(DbAttribute attribute) {

        // there can be more than one join for an attribute, only some being straight FKs
        // TODO: still kinda undeterministic.

        Optional<ForeignKey> fk = findAllRelationships(attribute)
                .stream()
                .map(this::createFk)
                .filter(Optional::isPresent)
                .findFirst()
                .orElse(Optional.empty());

        return new TargetAttribute(
                this,
                ASTDbPath.DB_PREFIX + attribute.getName(),
                attribute.getScale(),
                javaType(attribute),
                fk);
    }

    protected TargetAttribute createAttribute(DbRelationship relationship) {

        List<DbJoin> joins = relationship.getJoins();

        if (joins.size() > 1) {
            // TODO: support for multi-key to-one relationships, resolved to multiple TargetAttributes
            throw new LmRuntimeException("Multi-column FKs are not yet supported. Relationship: "
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
                createFk(relationship));
    }

    private Optional<ForeignKey> createFk(DbRelationship relationship) {

        // What we decide to be a FK or not must roughly match the algorithm in
        // TargetPropertyWriterService.createWriterFactory(..), or we won't be able to match the data columns with
        // object properties

        // TODO: there's no full correspondence, as TargetPropertyWriterService checks for object properties

        DbEntity targetDbEntity = relationship.getTargetEntity();

        if (relationship.isSourceIndependentFromTargetChange()
                || relationship.getJoins().size() > 1
                || targetDbEntity.getPrimaryKeys().size() > 1) {
            return Optional.empty();
        }

        Collection<ObjEntity> targetEntities = targetDbEntity.getDataMap().getMappedEntities(targetDbEntity);

        switch (targetEntities.size()) {
            case 0:
                return Optional.empty();
            case 1:
                return Optional.of(new ForeignKey(entityMap,
                        targetEntities.iterator().next(),
                        ASTDbPath.DB_PREFIX + relationship.getJoins().get(0).getTargetName()));
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
