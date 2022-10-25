package com.nhl.link.move.extractor;

import com.nhl.link.move.Execution;
import com.nhl.link.move.RowReader;

import java.util.List;

public class MultiExtractor implements Extractor {

	private final List<Extractor> extractors;

	public MultiExtractor(List<Extractor> extractors) {
		this.extractors = extractors;
	}

	@Override
	public RowReader getReader(Execution exec) {
		return new MultiExtractorRowReader(extractors, exec);
	}
}
