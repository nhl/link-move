package com.nhl.link.move.extractor.model;

import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.extractor.Extractor;

import java.util.Collection;
import java.util.Map;

/**
 * A model of a single {@link Extractor}.
 * 
 * @since 1.4
 */
public interface ExtractorModel {
	
	String DEFAULT_NAME = "default_extractor";

	String getName();

	String getType();

	/**
	 * @return Collection of connector IDs
	 * @since 2.2
     */
	Collection<String> getConnectorIds();

	Map<String, String> getProperties();

	RowAttribute[] getAttributes();
	
	/**
	 * Returns load timestamp for this model.
	 */
	long getLoadedOn();
}
