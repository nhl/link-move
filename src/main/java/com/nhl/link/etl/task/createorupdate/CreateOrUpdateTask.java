package com.nhl.link.etl.task.createorupdate;

import java.util.Iterator;
import java.util.List;

import org.apache.cayenne.ObjectContext;

import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowReader;
import com.nhl.link.etl.SyncToken;
import com.nhl.link.etl.batch.BatchProcessor;
import com.nhl.link.etl.batch.BatchRunner;
import com.nhl.link.etl.extract.Extractor;
import com.nhl.link.etl.extract.ExtractorParameters;
import com.nhl.link.etl.runtime.EtlRuntimeBuilder;
import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.token.ITokenManager;

/**
 * @since 1.3
 */
public class CreateOrUpdateTask<T> implements EtlTask {

	private String extractorName;
	private int batchSize;
	private ITargetCayenneService targetCayenneService;
	private ITokenManager tokenManager;
	private IExtractorService extractorService;
	private CreateOrUpdateSegmentProcessor<T> processor;

	public CreateOrUpdateTask(String extractorName, int batchSize, ITargetCayenneService targetCayenneService,
			ITokenManager tokenManager, IExtractorService extractorService, CreateOrUpdateSegmentProcessor<T> processor) {

		this.extractorName = extractorName;
		this.batchSize = batchSize;
		this.targetCayenneService = targetCayenneService;
		this.tokenManager = tokenManager;
		this.extractorService = extractorService;
		this.processor = processor;
	}

	@Override
	public Execution run() {
		return run(SyncToken.nullToken(extractorName));
	}

	@Override
	public Execution run(SyncToken token) {

		try (Execution execution = new Execution(token);) {

			BatchProcessor<Row> batchProcessor = createBatchProcessor(execution);
			ExtractorParameters extractorParams = createExtractorParameters(token);

			try (RowReader data = getRowReader(execution, extractorParams)) {
				BatchRunner.create(batchProcessor).withBatchSize(batchSize).run(data);
				tokenManager.saveToken(token);
			}

			return execution;
		}
	}

	protected ExtractorParameters createExtractorParameters(SyncToken token) {
		ExtractorParameters extractorParams = new ExtractorParameters();
		SyncToken startToken = tokenManager.previousToken(token);
		extractorParams.add(EtlRuntimeBuilder.START_TOKEN_VAR, startToken.getValue());
		extractorParams.add(EtlRuntimeBuilder.END_TOKEN_VAR, token.getValue());
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
