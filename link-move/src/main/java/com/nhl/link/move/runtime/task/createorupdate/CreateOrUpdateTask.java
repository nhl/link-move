package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.CountingRowReader;
import com.nhl.link.move.Execution;
import com.nhl.link.move.Row;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.batch.BatchProcessor;
import com.nhl.link.move.batch.BatchRunner;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.BaseTask;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

import java.util.List;
import java.util.Map;

/**
 * A task that reads streamed source data and creates/updates records in a
 * target DB.
 * 
 * @since 1.3
 */
public class CreateOrUpdateTask<T extends DataObject> extends BaseTask {

	private ExtractorName extractorName;
	private int batchSize;
	private ITargetCayenneService targetCayenneService;
	private IExtractorService extractorService;
	private CreateOrUpdateSegmentProcessor<T> processor;

	public CreateOrUpdateTask(ExtractorName extractorName, int batchSize, ITargetCayenneService targetCayenneService,
			IExtractorService extractorService, ITokenManager tokenManager, CreateOrUpdateSegmentProcessor<T> processor) {

		super(tokenManager);

		this.extractorName = extractorName;
		this.batchSize = batchSize;
		this.targetCayenneService = targetCayenneService;
		this.extractorService = extractorService;
		this.processor = processor;
	}

	@Override
	protected void run(Execution execution, Map<String, ?> params) {
		BatchProcessor<Row> batchProcessor = createBatchProcessor(execution);

		try (RowReader data = getRowReader(execution, params)) {
			BatchRunner.create(batchProcessor).withBatchSize(batchSize).run(data);
		}
	}

	@Override
	protected String name() {
		return "CreateOrUpdateTask:" + extractorName;
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
	protected RowReader getRowReader(Execution execution, Map<String, ?> extractorParams) {
		Extractor extractor = extractorService.getExtractor(extractorName);
		return new CountingRowReader(extractor.getReader(extractorParams), execution.getStats());
	}

}
