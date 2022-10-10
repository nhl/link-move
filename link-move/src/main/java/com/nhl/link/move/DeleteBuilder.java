package com.nhl.link.move;

import org.apache.cayenne.exp.Expression;

import com.nhl.link.move.mapper.Mapper;
import org.apache.cayenne.exp.property.Property;

/**
 * since 1.3
 */
public interface DeleteBuilder<T> {

	/**
	 * Creates a new task that will delete target objects not found in the
	 * source.
	 */
	LmTask task() throws IllegalStateException;

	DeleteBuilder<T> targetFilter(Expression filter);

	DeleteBuilder<T> sourceMatchExtractor(String extractorName);

	DeleteBuilder<T> matchBy(Mapper mapper);

	DeleteBuilder<T> matchBy(String... keyAttributes);

	DeleteBuilder<T> matchBy(Property<?>... keyAttributes);

	DeleteBuilder<T> matchById();

	DeleteBuilder<T> batchSize(int batchSize);

	DeleteBuilder<T> stageListener(Object listener);
}
