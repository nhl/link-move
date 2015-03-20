package com.nhl.link.etl;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Property;

import com.nhl.link.etl.mapper.Mapper;

/**
 * A builder of {@link EtlTask} that performs create-or-update synchronization.
 * 
 * @since 1.3
 */
public interface CreateOrUpdateBuilder<T> {

	/**
	 * Creates a new task based on the builder information.
	 */
	EtlTask task() throws IllegalStateException;

	/**
	 * Defines the name of the source data extractor.
	 * 
	 * @since 1.3
	 */
	CreateOrUpdateBuilder<T> sourceExtractor(String extractorName);

	/**
	 * @deprecated since 1.3 use {@link #sourceExtractor(String)}
	 */
	@Deprecated
	CreateOrUpdateBuilder<T> withExtractor(String extractorName);

	CreateOrUpdateBuilder<T> matchBy(Mapper<T> mapper);

	CreateOrUpdateBuilder<T> matchBy(String... keyAttributes);

	CreateOrUpdateBuilder<T> matchBy(Property<?>... keyAttributes);

	CreateOrUpdateBuilder<T> matchById(String idAttribute);

	/**
	 * Defines the number of records that are processed together as a single
	 * batch. If not specified, default size of 500 records is used.
	 * 
	 * @since 1.3
	 */
	CreateOrUpdateBuilder<T> batchSize(int batchSize);

	/**
	 * @deprecated since 1.3 use {@link #batchSize(int)}
	 */
	@Deprecated
	CreateOrUpdateBuilder<T> withBatchSize(int batchSize);

	CreateOrUpdateBuilder<T> withToOneRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute);

	CreateOrUpdateBuilder<T> withToOneRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute, String relationshipKeyAttribute);

	CreateOrUpdateBuilder<T> withToManyRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute);

	CreateOrUpdateBuilder<T> withToManyRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute, String relationshipKeyAttribute);

	/**
	 * Adds a listener of ETL target events.
	 */
	CreateOrUpdateBuilder<T> targetListener(TargetListener<T> listener);

	/**
	 * @deprecated since 1.3 in favor of {@link #targetListener(TargetListener)}
	 *             .
	 */
	@Deprecated
	CreateOrUpdateBuilder<T> withListener(TargetListener<T> listener);

}
