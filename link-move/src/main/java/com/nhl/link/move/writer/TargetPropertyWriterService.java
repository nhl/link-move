package com.nhl.link.move.writer;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.runtime.cayenne.CayenneCrossVersionBinaryCompat;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import org.apache.cayenne.di.Inject;
import org.apache.cayenne.map.ObjEntity;
import org.apache.cayenne.reflect.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @since 1.6
 */
public class TargetPropertyWriterService implements ITargetPropertyWriterService {

    private ITargetCayenneService targetCayenneService;

    private ConcurrentMap<Class<?>, TargetPropertyWriterFactory<?>> writerFactories;

    public TargetPropertyWriterService(@Inject ITargetCayenneService targetCayenneService) {
        this.targetCayenneService = targetCayenneService;
        this.writerFactories = new ConcurrentHashMap<>();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <T> TargetPropertyWriterFactory<T> getWriterFactory(Class<T> type) {
        return (TargetPropertyWriterFactory<T>) writerFactories.computeIfAbsent(type, this::createWriterFactory);
    }

    private <T> TargetPropertyWriterFactory<T> createWriterFactory(Class<T> type) {

        ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);
        if (entity == null) {
            throw new LmRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
        }

        TargetPropertyWriterFactory<T> writerFactory = new TargetPropertyWriterFactory<>(type, entity);
        ClassDescriptor descriptor = targetCayenneService.entityResolver().getClassDescriptor(entity.getName());

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

        CayenneCrossVersionBinaryCompat.pkAttributes(entity.getDbEntity()).forEach(writerFactory::initPkWriter);

        return writerFactory;
    }
}
