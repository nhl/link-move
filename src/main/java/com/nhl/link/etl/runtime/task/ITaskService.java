package com.nhl.link.etl.runtime.task;

import org.apache.cayenne.DataObject;

import com.nhl.link.etl.CreateOrUpdateBuilder;
import com.nhl.link.etl.DeleteBuilder;
import com.nhl.link.etl.SourceKeysBuilder;

public interface ITaskService {

	/**
	 * Returns a builder of create-or-update ETL synchronization task.
	 * 
	 * @since 1.3
	 */
	<T extends DataObject> CreateOrUpdateBuilder<T> createOrUpdate(Class<T> type);

	/**
	 * Returns a builder of target delete ETL synchronization task.
	 * 
	 * @since 1.3
	 */
	<T extends DataObject> DeleteBuilder<T> delete(Class<T> type);

	/**
	 * @since 1.3
	 */
	<T extends DataObject> SourceKeysBuilder extractSourceKeys(Class<T> type);

	/**
	 * @since 1.4
	 */
	SourceKeysBuilder extractSourceKeys(String targetEntityName);

	/**
	 * @deprecated since 1.3 use {@link #createOrUpdate(Class)}.
	 */
	@Deprecated
	<T extends DataObject> CreateOrUpdateBuilder<T> createTaskBuilder(Class<T> type);
}
