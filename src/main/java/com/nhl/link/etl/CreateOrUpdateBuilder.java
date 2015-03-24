package com.nhl.link.etl;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Property;

import com.nhl.link.etl.annotation.AfterSourceRowsConverted;
import com.nhl.link.etl.annotation.AfterSourcesMapped;
import com.nhl.link.etl.annotation.AfterTargetMerged;
import com.nhl.link.etl.load.LoadListener;
import com.nhl.link.etl.mapper.Mapper;

/**
 * A builder of an {@link EtlTask} that performs create-or-update
 * synchronization.
 * 
 * @since 1.3
 */
@SuppressWarnings("deprecation")
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

	CreateOrUpdateBuilder<T> matchBy(Mapper mapper);

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
	 * 
	 * @deprecated since 1.3 in favor of stage listeners.
	 */
	@Deprecated
	CreateOrUpdateBuilder<T> withListener(LoadListener<T> listener);

	/**
	 * Adds a listener of transformation stages of batch segments. It should
	 * have methods annotated with {@link AfterSourceRowsConverted},
	 * {@link AfterSourcesMapped}, {@link AfterTargetMerged},
	 * {@link AfterSourcesMapped}, etc.
	 * 
	 * @since 1.3
	 */
	CreateOrUpdateBuilder<T> stageListener(Object listener);

}
