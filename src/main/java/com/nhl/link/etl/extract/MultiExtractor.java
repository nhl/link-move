package com.nhl.link.etl.extract;

import java.util.List;

import com.nhl.link.etl.RowReader;

public class MultiExtractor implements Extractor {

	private List<Extractor> extractors;

	public MultiExtractor(List<Extractor> extractors) {
		this.extractors = extractors;
	}

	@Override
	public RowReader getReader(ExtractorParameters parameters) {
		return new MultiExtractorRowReader(extractors, parameters);
	}

}
