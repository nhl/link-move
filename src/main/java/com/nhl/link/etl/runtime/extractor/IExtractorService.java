package com.nhl.link.etl.runtime.extractor;

import com.nhl.link.etl.extractor.Extractor;
import com.nhl.link.etl.extractor.model.ExtractorName;

public interface IExtractorService {

	/**
	 * Returns an {@link Extractor} identified by {@link ExtractorName} or
	 * throws an exception if such an extractor is not available.
	 */
	Extractor getExtractor(ExtractorName name);
}
