package com.nhl.link.etl.transform;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.ObjectId;
import org.apache.cayenne.map.ObjEntity;

import java.util.List;
import java.util.Map;

public class CayenneCreateOrUpdateWithPKStrategy<T extends DataObject> extends DefaultCayenneCreateOrUpdateStrategy<T> {
	private final String primaryKeyAttribute;

	public CayenneCreateOrUpdateWithPKStrategy(List<RelationshipInfo> relationships, String primaryKeyAttribute) {
		super(relationships);
		this.primaryKeyAttribute = primaryKeyAttribute;
	}

	@Override
	public T create(ObjectContext context, Class<T> type, Map<String, Object> source) {
		Object primaryKey = source.get(primaryKeyAttribute);
		T target = context.newObject(type);
		update(context, source, target);
		ObjEntity objEntity = context.getEntityResolver().getObjEntity(type);
		target.setObjectId(new ObjectId(objEntity.getName(), primaryKeyAttribute, primaryKey));
		return target;
	}

	@Override
	public void update(ObjectContext context, Map<String, Object> source, T target) {
		for (Map.Entry<String, Object> e : source.entrySet()) {
			if (!e.getKey().equals(primaryKeyAttribute)) {
				writeProperty(context, target, e.getKey(), e.getValue());
			}
		}
	}
}
