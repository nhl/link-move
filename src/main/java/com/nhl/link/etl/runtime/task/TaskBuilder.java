package com.nhl.link.etl.runtime.task;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Property;

import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.load.LoadListener;
import com.nhl.link.etl.load.mapper.Mapper;

/**
 * @since 1.1
 */
public interface TaskBuilder<T> {

	/**
	 * Creates a new task based on the builder information.
	 */
	EtlTask task() throws IllegalStateException;

	TaskBuilder<T> withExtractor(String extractorName);

	TaskBuilder<T> matchBy(Mapper<T> mapper);

	TaskBuilder<T> matchBy(String... keyAttributes);

	TaskBuilder<T> matchBy(Property<?>... keyAttributes);

	/**
	 * @deprecated since 1.1 use {@link #matchById(String)}
	 */
	@Deprecated
	TaskBuilder<T> matchByPrimaryKey(String idAttribute);

	TaskBuilder<T> matchById(String idAttribute);

	TaskBuilder<T> withBatchSize(int batchSize);

	TaskBuilder<T> withToOneRelationship(String name, Class<? extends DataObject> relatedObjType, String keyAttribute);

	TaskBuilder<T> withToOneRelationship(String name, Class<? extends DataObject> relatedObjType, String keyAttribute,
			String relationshipKeyAttribute);

	TaskBuilder<T> withToManyRelationship(String name, Class<? extends DataObject> relatedObjType, String keyAttribute);

	TaskBuilder<T> withToManyRelationship(String name, Class<? extends DataObject> relatedObjType, String keyAttribute,
			String relationshipKeyAttribute);

	TaskBuilder<T> withListener(LoadListener<T> listener);
}
