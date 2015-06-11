package com.nhl.link.etl.extractor.model;

import java.util.Collection;

import com.nhl.link.etl.extractor.Extractor;

/**
 * A container that groups together one or more {@link Extractor}'s.
 * 
 * @since 1.4
 */
public interface ExtractorModelContainer {

	String getLocation();

	String getType();

	String getConnectorId();

	ExtractorModel getExtractor(String name);

	Collection<String> getExtractorNames();

	/**
	 * Returns load timestamp for this set of models.
	 */
	long getLoadedOn();
}
