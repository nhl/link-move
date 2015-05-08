package com.nhl.link.etl.runtime.task.createorupdate;

import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.query.SelectById;
import org.apache.cayenne.reflect.ArcProperty;
import org.apache.cayenne.reflect.AttributeProperty;
import org.apache.cayenne.reflect.ClassDescriptor;
import org.apache.cayenne.reflect.PropertyDescriptor;
import org.apache.cayenne.reflect.ToOneProperty;
import org.apache.cayenne.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nhl.link.etl.EtlRuntimeException;

public class CayenneCreateOrUpdateStrategy<T extends DataObject> implements CreateOrUpdateStrategy<T> {

	private static final Logger LOGGER = LoggerFactory.getLogger(CayenneCreateOrUpdateStrategy.class);

	@Override
	public T create(ObjectContext context, Class<T> type, Map<String, Object> source) {
		T target = context.newObject(type);
		update(context, source, target);
		return target;
	}

	@Override
	public boolean update(ObjectContext context, Map<String, Object> source, T target) {

		if (source.entrySet().isEmpty()) {
			return false;
		}

		boolean updated = false;

		// TODO: should we cache this lookup at a higher level?
		ClassDescriptor descriptor = context.getEntityResolver().getClassDescriptor(
				target.getObjectId().getEntityName());

		for (Map.Entry<String, Object> e : source.entrySet()) {
			updated = writeProperty(context, target, descriptor, e.getKey(), e.getValue()) || updated;
		}

		return updated;
	}

	protected boolean writeProperty(ObjectContext context, T target, ClassDescriptor descriptor, String property,
			Object value) {

		PropertyDescriptor pd = descriptor.getDeclaredProperty(property);

		if (pd instanceof AttributeProperty) {
			return writeAttribute(context, target, (AttributeProperty) pd, value);
		}

		if (pd instanceof ArcProperty) {

			if (pd instanceof ToOneProperty) {
				return writeRelationship(context, target, (ToOneProperty) pd, value);
			} else {
				throw new EtlRuntimeException("To-many relationships are not supported in ETL");
			}
		}

		LOGGER.warn("No attribute or relationship '{}' for entity '{}'", property, descriptor.getEntity().getName());
		return false;
	}

	private boolean writeRelationship(ObjectContext context, T target, ToOneProperty relationship, Object value) {

		boolean updated = false;
		DataObject object = resolveRelationshipObject(context, relationship, value);

		// explicitly prevent phantom updates... this would allow us to track
		// change status by looking inside ObjectContext.

		if (!Util.nullSafeEquals(readProperty(target, relationship.getName()), object)) {
			target.setToOneTarget(relationship.getName(), object, true);
			updated = true;
		}

		return updated;
	}

	private DataObject resolveRelationshipObject(ObjectContext context, ToOneProperty relationship, Object value) {
		return value != null ? (DataObject) SelectById
				.query(relationship.getTargetDescriptor().getObjectClass(), value).selectOne(context) : null;
	}

	private boolean writeAttribute(ObjectContext context, T target, AttributeProperty attribute, Object value) {

		boolean updated = false;

		// explicitly prevent phantom updates... this would allow us to track
		// change status by looking inside ObjectContext.

		if (!Util.nullSafeEquals(readProperty(target, attribute.getName()), value)) {
			target.writeProperty(attribute.getName(), value);
			updated = true;
		}

		return updated;
	}

	protected Object readProperty(T target, String property) {
		return target.readProperty(property);
	}
}
