package com.nhl.link.move.writer;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.exp.parser.ASTDbPath;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbJoin;
import org.apache.cayenne.map.DbRelationship;
import org.apache.cayenne.map.ObjEntity;
import org.apache.cayenne.reflect.AttributeProperty;
import org.apache.cayenne.reflect.ToOneProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * @since 1.6
 */
public class TargetPropertyWriterFactory<T> {

    public static final TargetPropertyWriter NULL_WRITER = (t, v) -> {
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(TargetPropertyWriterFactory.class);

    private Class<T> type;
    private ObjEntity entity;
    private Map<String, TargetPropertyWriter> writers = new ConcurrentHashMap<>();

    public TargetPropertyWriterFactory(Class<T> type, ObjEntity entity) {
        this.type = type;
        this.entity = entity;
    }

    private static String getSetterName(String propertyName) {
        return "set" + Character.toUpperCase(propertyName.charAt(0)) + propertyName.substring(1);
    }

    public TargetPropertyWriter getOrCreatePkWriter(DbAttribute pkAttribute) {

        if (!entity.getDbEntity().equals(pkAttribute.getEntity())) {
            throw new LmRuntimeException("Attribute belongs to different entity: " + pkAttribute.getName());
        }

        return getOrCreateWriter(
                pkAttribute.getName(),
                ASTDbPath.DB_PREFIX + pkAttribute.getName(),
                () -> new TargetPkPropertyWriter(pkAttribute)
        );
    }

    public TargetPropertyWriter getOrCreateWriter(AttributeProperty property) {

        if (!entity.equals(property.getAttribute().getEntity())) {
            throw new LmRuntimeException("Property belongs to a different entity: " + property.getName());
        }

        return getOrCreateWriter(
                property.getName(),
                ASTDbPath.DB_PREFIX + property.getAttribute().getDbAttributeName(),
                () -> new TargetAttributePropertyWriter(property)
        );
    }

    public TargetPropertyWriter getOrCreateWriter(ToOneProperty property) {

        if (!entity.equals(property.getRelationship().getSourceEntity())) {
            throw new LmRuntimeException("Property belongs to a different entity: " + property.getName());
        }

        List<DbRelationship> dbRelationships = property.getRelationship().getDbRelationships();
        if (dbRelationships.size() > 1) {
            // TODO: support for flattened to-one relationships
            LOGGER.info("TODO: not mapping db: path for a flattened relationship: " + property.getName());
            return NULL_WRITER;
        }

        DbRelationship dbRelationship = dbRelationships.get(0);
        List<DbJoin> joins = dbRelationship.getJoins();

        if (joins.size() > 1) {
            // TODO: support for multi-key to-one relationships
            LOGGER.info("TODO: not mapping db: path for a multi-key relationship: " + property.getName());
            return NULL_WRITER;
        }

        return getOrCreateWriter(
                property.getName(),
                ASTDbPath.DB_PREFIX + joins.get(0).getSourceName(),
                () -> new TargetToOnePropertyWriter(property)
        );
    }

    public TargetPropertyWriter getOrCreateWriter(String property) {
        return getOrCreateWriter(property, property, () -> NULL_WRITER);
    }

    public TargetPropertyWriter getOrCreateWriter(
            String propertyName,
            String dbName,
            Supplier<TargetPropertyWriter> defaultWriterSupplier) {

        return writers.computeIfAbsent(dbName, dbn -> createWriter(propertyName, defaultWriterSupplier));
    }

    private TargetPropertyWriter createWriter(String propertyName, Supplier<TargetPropertyWriter> defaultWriterSupplier) {

        // TODO: setter lookup does not check for the value type. E.g. a setter for to-one relationship will not
        // work properly if the value is presented as an FK (e.g. an "int" or a "long").
        Method setter = getSetter(propertyName);

        if (setter != null) {
            LOGGER.info(
                    "Found setter method for property '{}' in class: {}. Will create transient property writer...",
                    propertyName,
                    type.getName());

            return new TargetTransientPropertyWriter(setter);
        }

        return Objects.requireNonNull(defaultWriterSupplier.get(), () -> "Null property writer for " + propertyName);
    }

    private Method getSetter(String propertyName) {

        Method setter = null;
        String setterName = getSetterName(propertyName);
        for (Method m : type.getDeclaredMethods()) {
            if (Modifier.isPublic(m.getModifiers()) && setterName.equals(m.getName())) {
                setter = m;
                break;
            }
        }

        return setter;
    }
}
