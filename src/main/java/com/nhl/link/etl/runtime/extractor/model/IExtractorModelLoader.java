package com.nhl.link.etl.runtime.extractor.model;

import com.nhl.link.etl.extractor.model.ExtractorModelContainer;

public interface IExtractorModelLoader {

	/**
	 * Loads an {@link ExtractorModelContainer} from the specified location.
	 */
	ExtractorModelContainer load(String location);

	boolean needsReload(ExtractorModelContainer container);
}
