package com.nhl.link.etl.runtime.task;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Property;

import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.TargetListener;
import com.nhl.link.etl.mapper.Mapper;

/**
 * @since 1.3
 */
public interface CreateOrUpdateTaskBuilder<T> {

	/**
	 * Creates a new task based on the builder information.
	 */
	EtlTask task() throws IllegalStateException;

	/**
	 * Defines the name of the source data extractor.
	 * 
	 * @since 1.3
	 */
	CreateOrUpdateTaskBuilder<T> sourceExtractor(String extractorName);

	/**
	 * @deprecated since 1.3 use {@link #sourceExtractor(String)}
	 */
	@Deprecated
	CreateOrUpdateTaskBuilder<T> withExtractor(String extractorName);

	CreateOrUpdateTaskBuilder<T> matchBy(Mapper<T> mapper);

	CreateOrUpdateTaskBuilder<T> matchBy(String... keyAttributes);

	CreateOrUpdateTaskBuilder<T> matchBy(Property<?>... keyAttributes);

	CreateOrUpdateTaskBuilder<T> matchById(String idAttribute);

	/**
	 * Defines the number of records that are processed together as a single
	 * batch. If not specified, default size of 500 records is used.
	 * 
	 * @since 1.3
	 */
	CreateOrUpdateTaskBuilder<T> batchSize(int batchSize);

	/**
	 * @deprecated since 1.3 use {@link #batchSize(int)}
	 */
	@Deprecated
	CreateOrUpdateTaskBuilder<T> withBatchSize(int batchSize);

	CreateOrUpdateTaskBuilder<T> withToOneRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute);

	CreateOrUpdateTaskBuilder<T> withToOneRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute, String relationshipKeyAttribute);

	CreateOrUpdateTaskBuilder<T> withToManyRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute);

	CreateOrUpdateTaskBuilder<T> withToManyRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute, String relationshipKeyAttribute);

	/**
	 * Adds a listener of ETL target events.
	 */
	CreateOrUpdateTaskBuilder<T> targetListener(TargetListener<T> listener);

	/**
	 * @deprecated since 1.3 in favor of {@link #targetListener(TargetListener)}
	 *             .
	 */
	@Deprecated
	CreateOrUpdateTaskBuilder<T> withListener(TargetListener<T> listener);

}
