package com.nhl.link.move.extractor.model;

import com.nhl.link.move.RowAttribute;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @since 1.4
 */
public class MutableExtractorModel implements ExtractorModel {

	private String name;
	private String type;
	private Set<String> connectorIds;
	private long loadedOn;
	private RowAttribute[] attributes;
	private Map<String, String> properties;

	public MutableExtractorModel(String name) {
		this.name = name;
		this.connectorIds = new HashSet<>();
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
	public Collection<String> getConnectorIds() {
		return connectorIds;
	}

	@Override
	public RowAttribute[] getAttributes() {
		return attributes;
	}

	@Override
	public long getLoadedOn() {
		return loadedOn;
	}

	/**
	 * @since 2.2
     */
	public void addConnectorId(String connectorId) {
		this.connectorIds.add(connectorId);
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
