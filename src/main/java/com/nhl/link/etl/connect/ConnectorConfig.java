package com.nhl.link.etl.connect;

import java.util.HashMap;
import java.util.Map;

public class ConnectorConfig {
	private String name;

	private String type;

	private Map<String, String> properties;

	public ConnectorConfig(String name, String type) {
		this.name = name;
		this.type = type;
		this.properties = new HashMap<>();
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	public Map<String, String> getProperties() {
		return properties;
	}
}
