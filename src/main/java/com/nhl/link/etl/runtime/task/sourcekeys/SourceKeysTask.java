package com.nhl.link.etl.runtime.task.sourcekeys;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.nhl.link.etl.CountingRowReader;
import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowReader;
import com.nhl.link.etl.batch.BatchProcessor;
import com.nhl.link.etl.batch.BatchRunner;
import com.nhl.link.etl.extract.Extractor;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.task.BaseTask;
import com.nhl.link.etl.runtime.token.ITokenManager;

/**
 * An {@link EtlTask} that extracts all the keys from the source data store. The
 * result is stored in memory in the Execution object.
 * 
 * @since 1.3
 */
public class SourceKeysTask extends BaseTask {

	public static final String RESULT_KEY = SourceKeysTask.class.getName() + ".RESULT";

	private int batchSize;
	private IExtractorService extractorService;
	private String sourceExtractorName;
	private SourceKeysSegmentProcessor processor;

	public SourceKeysTask(String sourceExtractorName, int batchSize, IExtractorService extractorService,
			ITokenManager tokenManager, SourceKeysSegmentProcessor processor) {
		super(tokenManager);

		this.sourceExtractorName = sourceExtractorName;
		this.batchSize = batchSize;
		this.extractorService = extractorService;
		this.processor = processor;
	}

	@Override
	public Execution run(Map<String, ?> params) {

		if (params == null) {
			throw new NullPointerException("Null params");
		}

		try (Execution execution = new Execution("SourceKeysTask:" + sourceExtractorName, params);) {

			execution.setAttribute(RESULT_KEY, new HashSet<>());

			BatchProcessor<Row> batchProcessor = createBatchProcessor(execution);

			try (RowReader data = getRowReader(execution, params)) {
				BatchRunner.create(batchProcessor).withBatchSize(batchSize).run(data);
			}

			return execution;
		}
	}

	protected BatchProcessor<Row> createBatchProcessor(final Execution execution) {
		return new BatchProcessor<Row>() {

			@Override
			public void process(List<Row> rows) {
				processor.process(execution, new SourceKeysSegment(rows));
			}
		};
	}

	/**
	 * Returns a RowReader obtained from a named extractor and wrapped in a read
	 * stats counter.
	 */
	protected RowReader getRowReader(final Execution execution, Map<String, ?> extractorParams) {
		Extractor extractor = extractorService.getExtractor(sourceExtractorName);
		return new CountingRowReader(extractor.getReader(extractorParams), execution.getStats());
	}
}
