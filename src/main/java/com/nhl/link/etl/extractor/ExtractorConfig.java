package com.nhl.link.etl.extractor;

import java.util.HashMap;
import java.util.Map;

import com.nhl.link.etl.RowAttribute;

/**
 * Represents an abstract model of an {@link Extractor} of any kind.
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
