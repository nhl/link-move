package com.nhl.link.etl.task.createorupdate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.Cayenne;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.map.ObjEntity;
import org.apache.cayenne.query.SelectQuery;
import org.apache.cayenne.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.metadata.RelationshipInfo;
import com.nhl.link.etl.metadata.RelationshipType;

public class CayenneCreateOrUpdateStrategy<T extends DataObject> implements CreateOrUpdateStrategy<T> {

	private static final Logger LOGGER = LoggerFactory.getLogger(CayenneCreateOrUpdateStrategy.class);

	private final Map<String, RelationshipInfo> relationshipsByKeyAttributes;

	public CayenneCreateOrUpdateStrategy(List<RelationshipInfo> relationships) {
		relationshipsByKeyAttributes = new HashMap<>();
		for (RelationshipInfo relationship : relationships) {
			relationshipsByKeyAttributes.put(relationship.getKeyAttribute(), relationship);
		}
	}

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

		for (Map.Entry<String, Object> e : source.entrySet()) {
			updated = writeProperty(context, target, e.getKey(), e.getValue()) || updated;
		}

		return updated;
	}

	protected boolean writeProperty(ObjectContext context, T target, String property, Object value) {
		if (relationshipsByKeyAttributes.containsKey(property)) {
			return writeRelationship(context, target, relationshipsByKeyAttributes.get(property), value);
		} else {
			return writeBasicProperty(context, target, property, value);
		}
	}

	private boolean writeRelationship(ObjectContext context, T target, RelationshipInfo relationship, Object value) {

		boolean updated = false;
		DataObject object;

		if (value == null) {
			object = null;
		} else {
			object = resolveRelationshipObject(context, relationship, value);
			if (object == null) {
				// TODO: should we throw an exception here or just ignore?
				throw new EtlRuntimeException("Related object has not been found");
			}
		}

		if (relationship.getType() == RelationshipType.TO_ONE) {
			target.setToOneTarget(relationship.getName(), object, true);
			updated = true;
		} else if (relationship.getType() == RelationshipType.TO_MANY) {

			if (object == null) {
				// TODO: should we throw an exception here or just ignore?
				throw new EtlRuntimeException("Can't set to-many relationship '" + relationship.getName()
						+ "' to a null value");
			}

			target.addToManyTarget(relationship.getName(), object, true);
			updated = true;
		} else {
			throw new EtlRuntimeException("Unknown relationship type");
		}
		
		return updated;
	}

	private DataObject resolveRelationshipObject(ObjectContext context, RelationshipInfo relationship, Object value) {
		if (relationship.getRelationshipKeyAttribute() == null) {
			return Cayenne.objectForPK(context, relationship.getObjectType(), value);
		}
		return context.selectOne(new SelectQuery<>(relationship.getObjectType(), ExpressionFactory.matchExp(
				relationship.getRelationshipKeyAttribute(), value)));
	}

	private boolean writeBasicProperty(ObjectContext context, T target, String property, Object value) {
		// explicitly prevent phantom updates... this would allow us to track
		// change stats by looking inside ObjectContext.
		boolean updated = false;
		if (!Util.nullSafeEquals(readProperty(target, property), value)) {
			ObjEntity targetObjEntity = context.getEntityResolver().getObjEntity(target.getClass());
			if (targetObjEntity.getAttribute(property) != null) {
				target.writeProperty(property, value);
				updated = true;
			} else {
				LOGGER.warn("Unknown attribute '{}' for entity '{}'", property, target.getClass());
			}
		}

		return updated;
	}

	protected Object readProperty(T target, String property) {
		return target.readProperty(property);
	}
}
