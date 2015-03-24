package com.nhl.link.etl;

import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.Property;

import com.nhl.link.etl.mapper.Mapper;

/**
 * since 1.3
 */
public interface DeleteBuilder<T> {

	/**
	 * Creates a new task that will delete target objects not found in the
	 * source.
	 */
	EtlTask task() throws IllegalStateException;

	DeleteBuilder<T> targetFilter(Expression filter);

	DeleteBuilder<T> sourceMatchExtractor(String extractorName);

	DeleteBuilder<T> matchBy(Mapper mapper);

	DeleteBuilder<T> matchBy(String... keyAttributes);

	DeleteBuilder<T> matchBy(Property<?>... keyAttributes);

	DeleteBuilder<T> matchById(String idAttribute);

	DeleteBuilder<T> batchSize(int batchSize);

	DeleteBuilder<T> stageListener(Object listener);
}
