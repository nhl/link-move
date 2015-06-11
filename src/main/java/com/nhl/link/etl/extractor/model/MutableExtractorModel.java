package com.nhl.link.etl.extractor.model;

import java.util.HashMap;
import java.util.Map;

import com.nhl.link.etl.RowAttribute;

/**
 * @since 1.4
 */
public class MutableExtractorModel implements ExtractorModel {

	private String name;
	private String type;
	private String connectorId;
	private long loadedOn;
	private RowAttribute[] attributes;
	private Map<String, String> properties;

	public MutableExtractorModel(String name) {
		this.name = name;
		this.properties = new HashMap<>();
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getType() {
		return type;
	}

	@Override
	public String getConnectorId() {
		return connectorId;
	}

	@Override
	public RowAttribute[] getAttributes() {
		return attributes;
	}

	@Override
	public long getLoadedOn() {
		return loadedOn;
	}

	public void setConnectorId(String connectorId) {
		this.connectorId = connectorId;
	}

	public void setAttributes(RowAttribute... rowKeys) {
		this.attributes = rowKeys;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setLoadedOn(long loadedOn) {
		this.loadedOn = loadedOn;
	}

}
