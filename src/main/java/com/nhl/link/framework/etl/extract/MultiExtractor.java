package com.nhl.link.framework.etl.extract;

import java.util.List;

import com.nhl.link.framework.etl.RowReader;

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
