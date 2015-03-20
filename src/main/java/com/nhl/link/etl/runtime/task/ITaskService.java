package com.nhl.link.etl.runtime.task;

import org.apache.cayenne.DataObject;

import com.nhl.link.etl.CreateOrUpdateBuilder;

public interface ITaskService {

	/**
	 * Returns a builder of create-or-update ETL synchronization task.
	 * 
	 * @since 1.3
	 */
	<T extends DataObject> CreateOrUpdateBuilder<T> createOrUpdate(Class<T> type);

	/**
	 * @deprecated since 1.3 use {@link #createOrUpdate(Class)}.
	 */
	@Deprecated
	<T extends DataObject> CreateOrUpdateBuilder<T> createTaskBuilder(Class<T> type);
}
