package com.nhl.link.etl.extract;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowReader;

public class MultiExtractorRowReader implements RowReader {

	private static final Iterator<Row> EMPTY_ITERATOR = new Iterator<Row>() {

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public Row next() {
			throw new NoSuchElementException("No more elements");
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	};

	private List<Extractor> extractors;
	private Map<String, ?> parameters;

	private int currentExtractor;
	private RowReader currentReader;
	private Iterator<Row> currentIterator;

	public MultiExtractorRowReader(List<Extractor> extractors, Map<String, ?> parameters) {
		this.extractors = extractors;
		this.parameters = parameters;
	}

	@Override
	public void close() {
		// close last reader if any...
		if (currentReader != null) {
			currentReader.close();
			currentReader = null;
		}
	}

	@Override
	public Iterator<Row> iterator() {

		this.currentReader = null;
		this.currentExtractor = 0;
		this.currentIterator = EMPTY_ITERATOR;

		return new Iterator<Row>() {
			@Override
			public boolean hasNext() {
				return currentIterator().hasNext();
			}

			@Override
			public Row next() {
				return currentIterator().next();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	protected Iterator<Row> currentIterator() {

		if (!currentIterator.hasNext()) {

			if (currentExtractor >= extractors.size()) {
				return EMPTY_ITERATOR;
			}

			if (currentReader != null) {
				currentReader.close();
			}

			this.currentReader = extractors.get(currentExtractor++).getReader(parameters);
			this.currentIterator = currentReader.iterator();

			// recursion: if currentIterator is empty, we need to continue until
			// we find the one that is not, or exhaust all extractors
			if (!currentIterator.hasNext()) {
				return currentIterator();
			}
		}

		return currentIterator;
	}

}
