package com.nhl.link.move;

import org.apache.cayenne.exp.Property;

import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterSourcesMapped;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.mapper.Mapper;

/**
 * A builder of an {@link LmTask} that performs create-or-update
 * synchronization.
 * 
 * @since 1.3
 */
public interface CreateOrUpdateBuilder<T> {

	/**
	 * Creates a new task based on the builder information.
	 * 
	 * @return a new task based on the builder information.
	 */
	LmTask task() throws IllegalStateException;

	/**
	 * Defines the location and name of the source data extractor.
	 * 
	 * @param location
	 *            extractor configuration location, relative to some root known
	 *            to LinkMove.
	 * @param name
	 *            extractor name within configuration.
	 * @since 1.4
	 * @return this builder instance
	 */
	CreateOrUpdateBuilder<T> sourceExtractor(String location, String name);

	/**
	 * Defines the location of the source data extractor. The name of extractor
	 * is assumed to be "default_extractor".
	 * 
	 * @param location
	 *            extractor configuration location, relative to some root known
	 *            to LinkMove.
	 * @since 1.3
	 * @see #sourceExtractor(String, String)
	 * @return this builder instance
	 */
	CreateOrUpdateBuilder<T> sourceExtractor(String location);

	/**
	 * Instructs the task to match sources and targets using explicitly provided
	 * {@link Mapper}.
	 * 
	 * @param mapper
	 *            a custom {@link Mapper} to match sources against targets.
	 * @return this builder instance
	 */
	CreateOrUpdateBuilder<T> matchBy(Mapper mapper);

	/**
	 * Instructs the task to match sources and targets based on one or more
	 * attributes.
	 * 
	 * @param keyAttributes
	 *            target attributes to use in source-to-target mapper.
	 * @return this builder instance
	 */
	CreateOrUpdateBuilder<T> matchBy(String... keyAttributes);

	/**
	 * Instructs the task to match sources and targets based on one or more
	 * DataObject properties.
	 * 
	 * @param keyAttributes
	 *            target attributes to use in source-to-target mapper.
	 * @return this builder instance
	 */
	CreateOrUpdateBuilder<T> matchBy(Property<?>... keyAttributes);

	CreateOrUpdateBuilder<T> matchById();

	/**
	 * Defines the number of records that are processed together as a single
	 * batch. If not specified, default size of 500 records is used.
	 * 
	 * @param batchSize
	 *            the size of an internal processing batch.
	 * @since 1.3
	 * @return this builder instance
	 */
	CreateOrUpdateBuilder<T> batchSize(int batchSize);

	/**
	 * Adds a listener of transformation stages of batch segments. It should
	 * have methods annotated with {@link AfterSourceRowsConverted},
	 * {@link AfterSourcesMapped}, {@link AfterTargetsMerged},
	 * {@link AfterSourcesMapped}, etc.
	 * 
	 * @param listener
	 *            an annotated object that will receive events as the task
	 *            proceeds.
	 * @since 1.3
	 * @return this builder instance
	 */
	CreateOrUpdateBuilder<T> stageListener(Object listener);

}
