package com.nhl.link.etl.extractor;

import java.util.List;
import java.util.Map;

import com.nhl.link.etl.RowReader;

public class MultiExtractor implements Extractor {

	private List<Extractor> extractors;

	public MultiExtractor(List<Extractor> extractors) {
		this.extractors = extractors;
	}

	@Override
	public RowReader getReader(Map<String, ?> parameters) {
		return new MultiExtractorRowReader(extractors, parameters);
	}

}
