package com.nhl.link.etl.load.cayenne;

import com.nhl.link.etl.EtlRuntimeException;

import org.apache.cayenne.Cayenne;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.map.ObjEntity;
import org.apache.cayenne.query.SelectQuery;
import org.apache.cayenne.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultCayenneCreateOrUpdateStrategy<T extends DataObject> implements CayenneCreateOrUpdateStrategy<T> {
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCayenneCreateOrUpdateStrategy.class);

	private final Map<String, RelationshipInfo> relationshipsByKeyAttributes;

	public DefaultCayenneCreateOrUpdateStrategy(List<RelationshipInfo> relationships) {
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
	public void update(ObjectContext context, Map<String, Object> source, T target) {
		for (Map.Entry<String, Object> e : source.entrySet()) {
			writeProperty(context, target, e.getKey(), e.getValue());
		}
	}

	protected void writeProperty(ObjectContext context, T target, String property, Object value) {
		if (relationshipsByKeyAttributes.containsKey(property)) {
			writeRelationship(context, target, relationshipsByKeyAttributes.get(property), value);
		} else {
			writeBasicProperty(context, target, property, value);
		}
	}

	private void writeRelationship(ObjectContext context, T target, RelationshipInfo relationship,
	                               Object relationshipKey) {
		DataObject object = resolveRelationshipObject(context, relationship, relationshipKey);
		if (object == null) {
			// TODO: should we throw an exception here or just ignore?
			throw new EtlRuntimeException("Related object has not been found");
		}
		if (relationship.getType() == RelationshipType.TO_ONE) {
			target.setToOneTarget(relationship.getName(), object, true);
		} else if (relationship.getType() == RelationshipType.TO_MANY) {
			target.addToManyTarget(relationship.getName(), object, true);
		} else {
			throw new EtlRuntimeException("Unknown relationship type");
		}
	}

	private DataObject resolveRelationshipObject(ObjectContext context, RelationshipInfo relationship,
	                                             Object relationshipKey) {
		if (relationship.getRelationshipKeyAttribute() == null) {
			return Cayenne.objectForPK(context, relationship.getObjectType(), relationshipKey);
		}
		return context.selectOne(new SelectQuery<>(relationship.getObjectType(),
				ExpressionFactory.matchExp(relationship.getRelationshipKeyAttribute(), relationshipKey)));
	}

	private void writeBasicProperty(ObjectContext context, T target, String property, Object value) {
		// explicitly prevent phantom updates... this would allow us to track
		// change stats by looking inside ObjectContext.
		if (!Util.nullSafeEquals(readProperty(target, property), value)) {
			ObjEntity targetObjEntity = context.getEntityResolver().getObjEntity(target.getClass());
			if (targetObjEntity.getAttribute(property) != null) {
				target.writeProperty(property, value);
			} else {
				LOGGER.warn("Unknown attribute '{}' for entity '{}'", property, target.getClass());
			}
		}
	}

	protected Object readProperty(T target, String property) {
		return target.readProperty(property);
	}
}
