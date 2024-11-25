package com.nhl.link.move.writer;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import org.apache.cayenne.di.Inject;
import org.apache.cayenne.map.ObjEntity;
import org.apache.cayenne.reflect.AttributeProperty;
import org.apache.cayenne.reflect.ClassDescriptor;
import org.apache.cayenne.reflect.PropertyVisitor;
import org.apache.cayenne.reflect.ToManyProperty;
import org.apache.cayenne.reflect.ToOneProperty;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @since 1.6
 */
public class TargetPropertyWriterService implements ITargetPropertyWriterService {

    private final ITargetCayenneService targetCayenneService;
    private final ConcurrentMap<Object, TargetPropertyWriterFactory> writerFactories;

    public TargetPropertyWriterService(@Inject ITargetCayenneService targetCayenneService) {
        this.targetCayenneService = targetCayenneService;
        this.writerFactories = new ConcurrentHashMap<>();
    }

    @Override
    public TargetPropertyWriterFactory getWriterFactory(Class<?> type) {
        return writerFactories.computeIfAbsent(type, t -> createWriterFactory(((Class<?>) t)));
    }

    @Override
    public TargetPropertyWriterFactory getWriterFactory(String objEntityName) {
        return writerFactories.computeIfAbsent(objEntityName, name -> createWriterFactory(((String) name)));
    }

    private <T> TargetPropertyWriterFactory createWriterFactory(Class<T> type) {

        ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);
        if (entity == null) {
            throw new LmRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
        }

        ClassDescriptor descriptor = targetCayenneService.entityResolver().getClassDescriptor(entity.getName());
        TargetPropertyWriterFactory writerFactory = new TargetPropertyWriterFactory(type, descriptor);
        prepareWriterFactory(entity, descriptor, writerFactory);

        return writerFactory;
    }

    private TargetPropertyWriterFactory createWriterFactory(String objEntityName) {

        ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(objEntityName);
        if (entity == null) {
            throw new LmRuntimeException("ObjEntity " + objEntityName + " was not found in Cayenne");
        }

        ClassDescriptor descriptor = targetCayenneService.entityResolver().getClassDescriptor(entity.getName());
        TargetPropertyWriterFactory writerFactory = new TargetPropertyWriterFactory(descriptor);
        prepareWriterFactory(entity, descriptor, writerFactory);

        return writerFactory;
    }

    private static void prepareWriterFactory(ObjEntity entity, ClassDescriptor descriptor,
                                             TargetPropertyWriterFactory writerFactory) {
        // precompile all possible obj: and db: invariants
        // TODO: should we normalize the source map instead of doing this?
        descriptor.visitProperties(new PropertyVisitor() {

            @Override
            public boolean visitAttribute(AttributeProperty property) {
                writerFactory.initWriter(property);
                return true;
            }

            @Override
            public boolean visitToOne(ToOneProperty property) {
                if (!property.getRelationship().isSourceIndependentFromTargetChange()) {
                    writerFactory.initWriter(property);
                }
                return true;
            }

            @Override
            public boolean visitToMany(ToManyProperty property) {
                // nothing for ToMany
                return true;
            }
        });

        entity.getDbEntity().getPrimaryKeys().forEach(writerFactory::initPkWriter);
    }
}
