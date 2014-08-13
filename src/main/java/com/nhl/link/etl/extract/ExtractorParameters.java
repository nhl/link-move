package com.nhl.link.etl.extract;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Defines a map of parameters whose keys can be accessed either by name or by
 * position. This leaves it up to the template parsing engine to decide how to
 * bind the keys to a template. So the order of calling
 * {@link #add(String, Object)} method is significant.
 */
public class ExtractorParameters {

	private Map<String, Object> orderedParameters;

	public ExtractorParameters() {
		// LinkedHashMap ensures insertion order preservation...
		this.orderedParameters = new LinkedHashMap<>(4);
	}

	public void add(String name, Object value) {

		if (orderedParameters.containsKey(name)) {
			throw new IllegalArgumentException("Key '" + name + "' is already in use");
		}

		orderedParameters.put(name, value);
	}

	public Object[] asArray() {
		return orderedParameters.values().toArray();
	}

	public Map<String, Object> asMap() {
		return orderedParameters;
	}

}
