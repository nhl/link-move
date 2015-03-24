package com.nhl.link.etl.runtime.task;

import org.apache.cayenne.DataObject;

/**
 * A common superclass of various task builder.
 * 
 * @since 1.3
 */
public abstract class BaseTaskBuilder<T extends DataObject> {

	private static final int DEFAULT_BATCH_SIZE = 500;

	protected Class<T> type;
	protected int batchSize;

	public BaseTaskBuilder(Class<T> type) {
		this.type = type;
		this.batchSize = DEFAULT_BATCH_SIZE;
	}
}
