package com.nhl.link.etl.runtime.task;

import org.apache.cayenne.DataObject;

public interface ITaskService {

	/**
	 * Returns a builder of create-or-update ETL synchronization task.
	 * 
	 * @since 1.3
	 */
	<T extends DataObject> CreateOrUpdateTaskBuilder<T> createOrUpdate(Class<T> type);

	/**
	 * @deprecated since 1.3 use {@link #createOrUpdate(Class)}.
	 */
	@Deprecated
	<T extends DataObject> CreateOrUpdateTaskBuilder<T> createTaskBuilder(Class<T> type);
}
