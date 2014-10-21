package com.nhl.link.etl.load.cayenne;

import java.util.List;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

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
		target.getObjectId().getReplacementIdMap().put(primaryKeyAttribute, primaryKey);
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
