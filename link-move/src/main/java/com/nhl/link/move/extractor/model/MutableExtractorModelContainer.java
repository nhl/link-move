package com.nhl.link.move.extractor.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @since 1.4
 */
public class MutableExtractorModelContainer implements ExtractorModelContainer {

	private String location;
	private String type;
	private Set<String> connectorIds;
	private long loadedOn;

	private Map<String, ExtractorModel> extractors;

	public MutableExtractorModelContainer(String location) {
		this.location = location;

		this.connectorIds = new HashSet<>();
		// make sure the map is sorted for consistent iteration over
		// extractors...
		this.extractors = new TreeMap<>();
	}

	@Override
	public ExtractorModel getExtractor(String name) {
		return extractors.get(name);
	}

	@Override
	public Collection<String> getExtractorNames() {
		return Collections.unmodifiableCollection(extractors.keySet());
	}

	@Override
	public String getLocation() {
		return location;
	}

	@Override
	public String getType() {
		return type;
	}

	@Override
	public String getConnectorId() {
		if (connectorIds.isEmpty()) {
			// connector IDs can be missing
			return null;
		} if (connectorIds.size() == 1) {
			return connectorIds.iterator().next();
		}
		throw new IllegalStateException("Multiple connector IDs specified in model");
	}

	@Override
	public Collection<String> getConnectorIds() {
		return connectorIds;
	}

	@Override
	public long getLoadedOn() {
		return loadedOn;
	}

	public void addExtractor(String name, ExtractorModel extractor) {
		// TODO: check for dupes?
		extractors.put(name, extractor);
	}

	public void addConnectorId(String connectorId) {
		this.connectorIds.add(connectorId);
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setLoadedOn(long loadedOn) {
		this.loadedOn = loadedOn;
	}

}
