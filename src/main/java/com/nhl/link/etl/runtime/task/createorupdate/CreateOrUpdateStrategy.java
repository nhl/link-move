package com.nhl.link.etl.runtime.task.createorupdate;

import java.util.Map;

import org.apache.cayenne.ObjectContext;

public interface CreateOrUpdateStrategy<T> {

	T create(ObjectContext context, Class<T> type, Map<String, Object> source);

	boolean update(ObjectContext context, Map<String, Object> source, T target);
}
