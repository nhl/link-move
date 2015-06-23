package com.nhl.link.move.runtime.task;

/**
 * A common superclass of various task builder.
 * 
 * @since 1.3
 */
public abstract class BaseTaskBuilder {

	private static final int DEFAULT_BATCH_SIZE = 500;

	protected int batchSize;

	public BaseTaskBuilder() {
		this.batchSize = DEFAULT_BATCH_SIZE;
	}
}
