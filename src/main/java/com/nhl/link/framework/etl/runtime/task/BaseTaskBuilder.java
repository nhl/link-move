package com.nhl.link.framework.etl.runtime.task;

import java.util.Iterator;

import com.nhl.link.framework.etl.Execution;
import com.nhl.link.framework.etl.Row;
import com.nhl.link.framework.etl.RowReader;
import com.nhl.link.framework.etl.extract.Extractor;
import com.nhl.link.framework.etl.extract.ExtractorParameters;
import com.nhl.link.framework.etl.runtime.extract.IExtractorService;

public abstract class BaseTaskBuilder {

	private IExtractorService extractorService;

	BaseTaskBuilder(IExtractorService extractorService) {
		this.extractorService = extractorService;
	}

	/**
	 * Returns a RowReader obtained from a named extractor and wrapped in a read
	 * stats counter.
	 */
	protected RowReader getRowReader(final Execution execution, String extractorName,
			ExtractorParameters extractorParams) {

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
