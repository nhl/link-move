package com.nhl.link.etl.load;

import java.util.Map;

import com.nhl.link.etl.Execution;

/**
 * A listener for load-stage events.
 * 
 * @since 1.1
 */
public interface LoadListener<T> {

	void targetCreated(Execution e, Map<String, Object> source, T target);

	void targetUpdated(Execution e, Map<String, Object> source, T target);
}
