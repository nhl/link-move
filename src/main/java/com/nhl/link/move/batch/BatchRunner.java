package com.nhl.link.move.batch;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides a micro-batch execution facility that breaks an incoming stream into
 * segments of a predefined size and passes them in turn to the processor.
 *
 * @param <S>
 *            type of the batch source data.
 */
public class BatchRunner<S> {

	static final int DEFAULT_BATCH_SIZE = 500;

	private int batchSize;
	private BatchProcessor<S> processor;

	public static <S> BatchRunner<S> create(BatchProcessor<S> processor) {
		return new BatchRunner<>(processor);
	}

	private BatchRunner(BatchProcessor<S> processor) {
		this.batchSize = DEFAULT_BATCH_SIZE;
		this.processor = processor;
	}

	public BatchRunner<S> withBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * Splits the iterator into segments according to 'batchSize' property and
	 * runs each segment via a callback {@link BatchProcessor}.
	 */
	public void run(Iterable<S> source) {

		List<S> batch = new ArrayList<>(batchSize);

		for (S s : source) {

			batch.add(s);

			if (batch.size() % batchSize == 0) {

				processor.process(batch);
				batch.clear();
			}
		}

		if (!batch.isEmpty()) {
			processor.process(batch);
		}
	}
}
