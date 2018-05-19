package com.nhl.link.move.runtime.targetmodel;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.dba.TypesMapping;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.exp.parser.ASTDbPath;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbJoin;
import org.apache.cayenne.map.DbRelationship;
import org.apache.cayenne.map.ObjAttribute;
import org.apache.cayenne.map.ObjEntity;
import org.apache.cayenne.map.PathComponent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @since 2.6
 */
public class DefaultTargetEntity implements TargetEntity {

    static final TargetAttribute INVALID_ATTRIBUTE = new TargetAttribute(null, -1, null) {
        @Override
        public String getNormalizedPath() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getScale() {
            throw new UnsupportedOperationException();
        }
    };

    private ObjEntity entity;
    // may contain more than one entry for a single logical attribute (for normalized and un-normalized paths).
    private Map<String, TargetAttribute> attributes;

    public DefaultTargetEntity(ObjEntity entity) {
        this.entity = entity;
        this.attributes = new ConcurrentHashMap<>();
    }


    @Override
    public TargetAttribute getAttribute(String path) {
        TargetAttribute attribute = attributes.computeIfAbsent(path, this::createAttribute);
        return attribute == INVALID_ATTRIBUTE ? null : attribute;
    }

    protected TargetAttribute createAttribute(String path) {

        if (!hasAttributeOrRelationship(path)) {
            return INVALID_ATTRIBUTE;
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

        if (c.getAttribute() != null) {
            return createAttribute(c.getAttribute());
        }

        DbRelationship dbRelationship = c.getRelationship();
        List<DbJoin> joins = dbRelationship.getJoins();

        if (joins.size() > 1) {
            // TODO: support for multi-key to-one relationships
            throw new LmRuntimeException("Multi-column FKs are not yet supported. Path: " + dbExp);
        }

        // previous implementation used target attribute of a join to map Java class and scale... I think this was
        // wrong, but maybe there were some edge cases?
        return createAttribute(joins.get(0).getSource());
    }

    private boolean hasAttributeOrRelationship(String path) {
        if (path.startsWith(ASTDbPath.DB_PREFIX)) {
            path = path.substring(ASTDbPath.DB_PREFIX.length());
            return entity.getDbEntity().getAttributeMap().containsKey(path)
                    || entity.getDbEntity().getRelationshipMap().containsKey(path);
        } else {
            return entity.getAttributeMap().containsKey(path)
                    || entity.getRelationshipMap().containsKey(path);
        }
    }

    protected TargetAttribute createAttribute(DbAttribute attribute) {
        return new TargetAttribute(
                ASTDbPath.DB_PREFIX + attribute.getName(),
                attribute.getScale(),
                javaType(attribute));
    }

    private String javaType(DbAttribute attribute) {
        ObjAttribute objAttribute = entity.getAttributeForDbAttribute(attribute);
        return (objAttribute != null)
                ? objAttribute.getType()
                : TypesMapping.getJavaBySqlType(attribute.getType());
    }
}
