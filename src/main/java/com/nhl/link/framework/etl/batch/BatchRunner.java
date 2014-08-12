package com.nhl.link.framework.etl.batch;

import java.util.ArrayList;
import java.util.List;

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
	 * Splits the iteration into batched segments according to 'batchSize'
	 * property and runs each segment via a callback {@link BatchProcessor}.
	 */
	public void run(Iterable<T> objects) {

		List<T> batch = new ArrayList<>(batchSize);

		for (T o : objects) {

			batch.add(o);

			if (batch.size() % batchSize == 0) {

				processor.process(batch);
				batch.clear();
			}
		}

		if (!batch.isEmpty()) {
			processor.process(batch);
		}
	}

	public <S> void run(final Iterable<S> source, final BatchConverter<S, T> converter) {

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
