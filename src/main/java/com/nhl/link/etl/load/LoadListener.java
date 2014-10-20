package com.nhl.link.etl.load;

import java.util.Map;

/**
 * A listener for load-stage events.
 * 
 * @since 1.1
 */
public interface LoadListener<T> {

	void targetCreated(Map<String, Object> source, T target);

	void targetUpdated(Map<String, Object> source, T target);
}
