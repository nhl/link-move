package com.nhl.link.move.runtime.extractor.model;

import com.nhl.link.move.extractor.model.ExtractorModelContainer;

public interface IExtractorModelLoader {

	/**
	 * Loads an {@link ExtractorModelContainer} from the specified location.
	 */
	ExtractorModelContainer load(String location);

	boolean needsReload(ExtractorModelContainer container);
}
