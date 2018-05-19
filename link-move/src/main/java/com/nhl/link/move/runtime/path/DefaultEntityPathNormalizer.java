package com.nhl.link.move.runtime.path;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.valueconverter.ValueConverterFactory;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @since 2.6
 */
public class DefaultEntityPathNormalizer implements EntityPathNormalizer {

    private ObjEntity entity;
    private ConcurrentMap<String, AttributeInfo> cachedAttributes;
    private ValueConverterFactory converterFactory;

    public DefaultEntityPathNormalizer(ObjEntity entity, ValueConverterFactory converterFactory) {
        this.entity = entity;
        this.converterFactory = converterFactory;
        this.cachedAttributes = new ConcurrentHashMap<>();
    }

    @Override
    public String normalize(String path) {

        if (!hasAttribute(entity, path)) {
            return path;
        }

        if (path == null) {
            throw new NullPointerException("Null path. Entity: " + entity.getName());
        }

        return getAttributeInfo(path).getNormalizedPath();
    }

    @Override
    public Object normalizeValue(String path, Object value) {

        if (!hasAttribute(entity, path)) {
            return value;
        }

        if (value == null) {
            return null;
        }

        AttributeInfo attributeInfo = getAttributeInfo(path);

        ObjAttribute objAttribute = entity.getAttributeForDbAttribute(attributeInfo.getTarget());
        String javaType = (objAttribute != null)
                ? objAttribute.getType()
                : TypesMapping.getJavaBySqlType(attributeInfo.getType());
        int scale = attributeInfo.getTarget().getScale();

        return converterFactory.getConverter(javaType).convert(value, scale);
    }

    private boolean hasAttribute(ObjEntity entity, String path) {
        if (path.startsWith(ASTDbPath.DB_PREFIX)) {
            path = path.substring(ASTDbPath.DB_PREFIX.length());
            return entity.getDbEntity().getAttributeMap().containsKey(path)
                    || entity.getDbEntity().getRelationshipMap().containsKey(path);
        } else {
            return entity.getAttributeMap().containsKey(path) || entity.getRelationshipMap().containsKey(path);
        }
    }

    private AttributeInfo getAttributeInfo(String path) {
        return cachedAttributes.computeIfAbsent(path, this::doNormalize);
    }

    private AttributeInfo doNormalize(String path) {

        Expression dbExp = entity.translateToDbPath(ExpressionFactory.exp(path));

        List<PathComponent<DbAttribute, DbRelationship>> components = new ArrayList<>(2);
        for (PathComponent<DbAttribute, DbRelationship> c : entity.getDbEntity().resolvePath(dbExp,
                Collections.emptyMap())) {
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
            return AttributeInfo.forAttribute(c.getAttribute());
        }

        DbRelationship dbRelationship = c.getRelationship();

        List<DbJoin> joins = dbRelationship.getJoins();

        // TODO: logic duplication from DefaultCreateOrUpdateBuilder...
        if (joins.size() > 1) {
            // TODO: support for multi-key to-one relationships
            throw new LmRuntimeException("Multi-column FKs are not yet supported. Path: " + dbExp);
        } else {
            return AttributeInfo.forRelationship(joins.get(0));
        }
    }

    private static abstract class AttributeInfo {

        public static AttributeInfo forAttribute(DbAttribute attribute) {
            return new AttributeInfo() {
                @Override
                public String getNormalizedPath() {
                    return ASTDbPath.DB_PREFIX + attribute.getName();
                }

                @Override
                public int getType() {
                    return attribute.getType();
                }

                @Override
                public DbAttribute getTarget() {
                    return attribute;
                }
            };
        }

        public static AttributeInfo forRelationship(DbJoin join) {
            return new AttributeInfo() {
                @Override
                public String getNormalizedPath() {
                    return ASTDbPath.DB_PREFIX + join.getSourceName();
                }

                @Override
                public int getType() {
                    return join.getSource().getType();
                }

                @Override
                public DbAttribute getTarget() {
                    return join.getTarget();
                }
            };
        }

        public abstract String getNormalizedPath();

        public abstract int getType();

        public abstract DbAttribute getTarget();
    }
}
