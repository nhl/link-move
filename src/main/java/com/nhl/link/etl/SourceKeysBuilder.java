package com.nhl.link.etl;

import com.nhl.link.etl.mapper.Mapper;

/**
 * A builder of an {@link EtlTask} that extracts all the keys from the source
 * data store.
 * 
 * @since 1.3
 */
public interface SourceKeysBuilder {

	/**
	 * Creates a new task based on the builder information.
	 */
	EtlTask task() throws IllegalStateException;

	/**
	 * Defines the name of the source data extractor.
	 */
	SourceKeysBuilder sourceExtractor(String extractorName);

	/**
	 * Defines the number of records that are processed together as a single
	 * batch. If not specified, default size of 500 records is used.
	 */
	SourceKeysBuilder batchSize(int batchSize);

	SourceKeysBuilder matchBy(Mapper mapper);

	SourceKeysBuilder matchBy(String... columns);

}
