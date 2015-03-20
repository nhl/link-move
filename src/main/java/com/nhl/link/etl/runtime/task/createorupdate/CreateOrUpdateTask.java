package com.nhl.link.etl.runtime.task.createorupdate;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cayenne.ObjectContext;

import com.nhl.link.etl.Execution;
import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowReader;
import com.nhl.link.etl.batch.BatchProcessor;
import com.nhl.link.etl.batch.BatchRunner;
import com.nhl.link.etl.extract.Extractor;
import com.nhl.link.etl.extract.ExtractorParameters;
import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.task.BaseTask;
import com.nhl.link.etl.runtime.token.ITokenManager;

/**
 * A task that reads streamed source data and creates/updates records in a
 * target DB.
 * 
 * @since 1.3
 */
public class CreateOrUpdateTask<T> extends BaseTask {

	private String extractorName;
	private int batchSize;
	private ITargetCayenneService targetCayenneService;
	private IExtractorService extractorService;
	private CreateOrUpdateSegmentProcessor<T> processor;

	public CreateOrUpdateTask(String extractorName, int batchSize, ITargetCayenneService targetCayenneService,
			IExtractorService extractorService, ITokenManager tokenManager, CreateOrUpdateSegmentProcessor<T> processor) {

		super(tokenManager);

		this.extractorName = extractorName;
		this.batchSize = batchSize;
		this.targetCayenneService = targetCayenneService;
		this.extractorService = extractorService;
		this.processor = processor;
	}

	@Override
	public Execution run(Map<String, Object> params) {

		try (Execution execution = new Execution("CreateOrUpdateTask:" + extractorName, params);) {

			BatchProcessor<Row> batchProcessor = createBatchProcessor(execution);
			ExtractorParameters extractorParams = createExtractorParameters(params);

			try (RowReader data = getRowReader(execution, extractorParams)) {
				BatchRunner.create(batchProcessor).withBatchSize(batchSize).run(data);
			}

			return execution;
		}
	}

	protected ExtractorParameters createExtractorParameters(Map<String, Object> params) {
		ExtractorParameters extractorParams = new ExtractorParameters();

		for (Entry<String, Object> e : params.entrySet()) {
			extractorParams.add(e.getKey(), e.getValue());
		}

		return extractorParams;
	}

	protected BatchProcessor<Row> createBatchProcessor(final Execution execution) {
		return new BatchProcessor<Row>() {

			ObjectContext context = targetCayenneService.newContext();

			@Override
			public void process(List<Row> rows) {
				processor.process(execution, new CreateOrUpdateSegment<T>(context, rows));
			}
		};
	}

	/**
	 * Returns a RowReader obtained from a named extractor and wrapped in a read
	 * stats counter.
	 */
	protected RowReader getRowReader(final Execution execution, ExtractorParameters extractorParams) {

		Extractor extractor = extractorService.getExtractor(extractorName);
		final RowReader reader = extractor.getReader(extractorParams);
		return new RowReader() {

			@Override
			public Iterator<Row> iterator() {
				final Iterator<Row> it = reader.iterator();

				return new Iterator<Row>() {
					@Override
					public boolean hasNext() {
						return it.hasNext();
					}

					@Override
					public void remove() {
						it.remove();
					}

					@Override
					public Row next() {
						execution.incrementExtracted(1);
						return it.next();
					}
				};
			}

			@Override
			public void close() {
				reader.close();
			}
		};
	}

}
