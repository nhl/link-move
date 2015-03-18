package com.nhl.link.etl.batch;

import java.util.ArrayList;
import java.util.List;

import com.nhl.link.etl.transform.PassThroughConverter;

/**
 * Provides a micro-batch execution facility that breaks an incoming stream into
 * segments of a predefined size and passes them in turn to the processor.
 *
 * @param <T>
 *            type of the batch source data.
 */
public class BatchRunner<T> {

	static final int DEFAULT_BATCH_SIZE = 500;

	private int batchSize;
	private BatchProcessor<T> processor;

	public static <T> BatchRunner<T> create(BatchProcessor<T> processor) {
		return new BatchRunner<T>(processor);
	}

	private BatchRunner(BatchProcessor<T> processor) {
		this.batchSize = DEFAULT_BATCH_SIZE;
		this.processor = processor;
	}

	public BatchRunner<T> withBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * Splits the iterator into segments according to 'batchSize' property and
	 * runs each segment via a callback {@link BatchProcessor}.
	 */
	public void run(Iterable<T> source) {
		run(source, PassThroughConverter.<T> instance());
	}

	/**
	 * Splits the iterator into segments according to 'batchSize' property, then
	 * converts each segment from type S to the processor type T using provided
	 * converter and finally executes a converted segment with
	 * {@link BatchProcessor}.
	 */
	public <S> void run(final Iterable<S> source, BatchConverter<S, T> converter) {

		// reuse converted objects to minimize GC between batches

		List<T> templates = new ArrayList<>(batchSize);
		List<T> batch = new ArrayList<>(batchSize);

		for (int i = 0; i < batchSize; i++) {
			templates.add(converter.createTemplate());
		}

		int i = 0;

		for (S s : source) {

			T t = converter.fromTemplate(s, templates.get(i++ % batchSize));
			batch.add(t);

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
