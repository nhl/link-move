package com.nhl.link.etl.batch;

import java.util.ArrayList;
import java.util.List;

import com.nhl.link.etl.batch.converter.PassThroughConverter;

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
		run(source, PassThroughConverter.<S> instance());
	}

	/**
	 * Splits the iterator into segments according to 'batchSize' property, then
	 * converts each segment from type S to the processor type T using provided
	 * converter and finally executes a converted segment with
	 * {@link BatchProcessor}.
	 * 
	 * @param <R>
	 *            a "raw" source type that is preconverted to S by the supplied
	 *            {@link BatchConverter} before being passed to
	 *            {@link BatchProcessor}.
	 */
	public <R> void run(final Iterable<R> source, BatchConverter<R, S> converter) {

		// reuse converted objects to minimize GC between batches

		List<S> templates = new ArrayList<>(batchSize);
		List<S> batch = new ArrayList<>(batchSize);

		for (int i = 0; i < batchSize; i++) {
			templates.add(converter.createTemplate());
		}

		int i = 0;

		for (R s : source) {

			S t = converter.fromTemplate(s, templates.get(i++ % batchSize));
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
