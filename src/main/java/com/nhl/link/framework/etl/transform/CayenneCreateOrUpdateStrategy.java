package com.nhl.link.framework.etl.transform;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

import java.util.Map;

public interface CayenneCreateOrUpdateStrategy<T extends DataObject> {
	T create(ObjectContext context, Class<T> type, Map<String, Object> source);

	void update(ObjectContext context, Map<String, Object> source, T target);
}
