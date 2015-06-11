package com.nhl.link.etl.runtime.extractor.model;

import com.nhl.link.etl.extractor.model.ExtractorModel;
import com.nhl.link.etl.extractor.model.ExtractorName;

/**
 * A service that provides access to extractor models regardless of their
 * location.
 * 
 * @since 1.4
 */
public interface IExtractorModelService {

	/**
	 * Returns an {@link ExtractorModel} matching the name.
	 * 
	 * 
	 * @param a
	 *            a fully-qualified name of the extractor within container.
	 */
	ExtractorModel get(ExtractorName name);
}
