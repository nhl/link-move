package com.nhl.link.move.extractor;

import com.nhl.link.move.RowReader;

import java.util.List;
import java.util.Map;

public class MultiExtractor implements Extractor {

	private final List<Extractor> extractors;

	public MultiExtractor(List<Extractor> extractors) {
		this.extractors = extractors;
	}

	@Override
	public RowReader getReader(Map<String, ?> parameters) {
		return new MultiExtractorRowReader(extractors, parameters);
	}

}
