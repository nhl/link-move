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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @since 2.6
 */
public class DefaultEntityPathNormalizer implements EntityPathNormalizer {

    private ObjEntity entity;
    private Map<String, AttributeInfo> attributes;
    private ValueConverterFactory converterFactory;

    public DefaultEntityPathNormalizer(ObjEntity entity, ValueConverterFactory converterFactory) {
        this.entity = entity;
        this.converterFactory = converterFactory;
        this.attributes = new ConcurrentHashMap<>();
    }

    @Override
    public String normalize(String path) {
        Objects.requireNonNull(path, () -> "Null path. Entity: " + entity.getName());
        AttributeInfo attributeInfo = getAttributeInfo(path);
        return attributeInfo == AttributeInfo.INVALID ? path : attributeInfo.getNormalizedPath();
    }

    @Override
    public Object normalizeValue(String path, Object value) {

        if (value == null) {
            return null;
        }

        AttributeInfo attributeInfo = getAttributeInfo(path);
        if (attributeInfo == AttributeInfo.INVALID) {
            return value;
        }

        ObjAttribute objAttribute = entity.getAttributeForDbAttribute(attributeInfo.getTarget());
        String javaType = (objAttribute != null)
                ? objAttribute.getType()
                : TypesMapping.getJavaBySqlType(attributeInfo.getType());
        int scale = attributeInfo.getTarget().getScale();

        return converterFactory.getConverter(javaType).convert(value, scale);
    }

    private AttributeInfo getAttributeInfo(String path) {
        return attributes.computeIfAbsent(path, this::createAttributeInfo);
    }

    private AttributeInfo createAttributeInfo(String path) {

        if (!hasAttributeOrRelationship(path)) {
            return AttributeInfo.INVALID;
        }

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

    private static abstract class AttributeInfo {

        static final AttributeInfo INVALID = new AttributeInfo() {
            @Override
            public String getNormalizedPath() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getType() {
                throw new UnsupportedOperationException();
            }

            @Override
            public DbAttribute getTarget() {
                throw new UnsupportedOperationException();
            }
        };


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
