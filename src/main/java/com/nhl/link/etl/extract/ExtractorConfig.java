package com.nhl.link.etl.extract;

import java.util.HashMap;
import java.util.Map;

import com.nhl.link.etl.RowAttribute;

/**
 * A thread-safe config for an {@link Extractor}. The config can be externally
 * updated by calling {@link #setProperties(Map)}.
 */
public class ExtractorConfig {

	private String name;
	private String type;
	private String connectorId;
	private RowAttribute[] attributes;
	private Map<String, String> properties;

	public ExtractorConfig(String name) {
		this.name = name;
		this.properties = new HashMap<>();
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	public String getConnectorId() {
		return connectorId;
	}

	public void setConnectorId(String connectorId) {
		this.connectorId = connectorId;
	}

	public RowAttribute[] getAttributes() {
		return attributes;
	}

	public void setAttributes(RowAttribute... rowKeys) {
		this.attributes = rowKeys;
	}

	public void setType(String type) {
		this.type = type;
	}

}
