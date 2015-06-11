package com.nhl.link.etl.extractor.model;

import java.util.Map;

import com.nhl.link.etl.RowAttribute;
import com.nhl.link.etl.extractor.Extractor;

/**
 * A model of a single {@link Extractor}.
 * 
 * @since 1.4
 */
public interface ExtractorModel {

	String getName();

	String getType();

	String getConnectorId();

	Map<String, String> getProperties();

	RowAttribute[] getAttributes();
	
	/**
	 * Returns load timestamp for this model.
	 */
	long getLoadedOn();
}
