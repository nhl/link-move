package com.nhl.link.etl.runtime.extractor;

import com.nhl.link.etl.extractor.Extractor;

public interface IExtractorService {

	/**
	 * Returns a named {@link Extractor} or throws an exception if such an
	 * extractor is unmapped.
	 */
	Extractor getExtractor(String name);
}
