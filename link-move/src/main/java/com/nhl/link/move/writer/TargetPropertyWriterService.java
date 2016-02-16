package com.nhl.link.move.writer;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import org.apache.cayenne.di.Inject;
import org.apache.cayenne.map.DbAttribute;
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

    private ITargetCayenneService targetCayenneService;

    private ConcurrentMap<Class<?>, TargetPropertyWriterFactory<?>> writerFactories;

    public TargetPropertyWriterService(@Inject ITargetCayenneService targetCayenneService) {
        this.targetCayenneService = targetCayenneService;
        this.writerFactories = new ConcurrentHashMap<>();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <T> TargetPropertyWriterFactory<T> getWriterFactory(Class<T> type) {

        if (targetCayenneService.entityResolver().getObjEntity(type) == null) {
			throw new LmRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
		}

        TargetPropertyWriterFactory<T> writerFactory = (TargetPropertyWriterFactory<T>) writerFactories.get(type);
        if (writerFactory == null) {
            writerFactory = createWriterFactory(type);
            TargetPropertyWriterFactory existing = writerFactories.putIfAbsent(type, writerFactory);
            if (existing != null) {
                writerFactory = existing;
            }
        }
        return writerFactory;
    }

    private <T> TargetPropertyWriterFactory<T> createWriterFactory(Class<T> type) {

        ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);
        final TargetPropertyWriterFactory<T> writerFactory = new TargetPropertyWriterFactory<>(type, entity);
        ClassDescriptor descriptor = targetCayenneService.entityResolver().getClassDescriptor(entity.getName());

		// TODO: instead of providing mappings for all possible obj: and db:
		// invariants, should we normalize the source map instead?

		descriptor.visitProperties(new PropertyVisitor() {

			@Override
			public boolean visitAttribute(AttributeProperty property) {
				writerFactory.getOrCreateWriter(property);
				return true;
			}

			@Override
			public boolean visitToOne(ToOneProperty property) {
				writerFactory.getOrCreateWriter(property);
				return true;
			}

			@Override
			public boolean visitToMany(ToManyProperty property) {
				// nothing for ToMany
				return true;
			}
		});

		for (DbAttribute pk : entity.getDbEntity().getPrimaryKeys()) {
            writerFactory.getOrCreatePkWriter(pk);
		}

        return writerFactory;
	}
}
