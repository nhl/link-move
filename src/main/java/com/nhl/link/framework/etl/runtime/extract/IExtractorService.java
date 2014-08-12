package com.nhl.link.framework.etl.runtime.extract;

import com.nhl.link.framework.etl.extract.Extractor;

public interface IExtractorService {

	/**
	 * Returns a named {@link Extractor} or throws an exception if such
	 * DataSource is unmapped.
	 */
	Extractor getExtractor(String name);
}
