package com.nhl.link.move.extractor.model;

import java.util.Collection;

import com.nhl.link.move.extractor.Extractor;

/**
 * A container that groups together one or more {@link Extractor}'s.
 * 
 * @since 1.4
 */
public interface ExtractorModelContainer {

	String getLocation();

	String getType();

	@Deprecated
	String getConnectorId();

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
