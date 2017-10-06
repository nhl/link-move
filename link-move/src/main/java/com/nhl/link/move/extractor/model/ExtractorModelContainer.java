package com.nhl.link.move.extractor.model;

import com.nhl.link.move.extractor.Extractor;

import java.util.Collection;

/**
 * A container that groups together one or more {@link Extractor}'s.
 * 
 * @since 1.4
 */
public interface ExtractorModelContainer {

	String getLocation();

	String getType();

	/**
	 * @return Collection of connector IDs
	 * @since 2.2
     */
	Collection<String> getConnectorIds();

	ExtractorModel getExtractor(String name);

	Collection<String> getExtractorNames();

	/**
	 * Returns load timestamp for this set of models.
	 */
	long getLoadedOn();
}
