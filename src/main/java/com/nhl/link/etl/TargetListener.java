package com.nhl.link.etl;

import java.util.Map;

/**
 * A listener for target merge events.
 * 
 * @since 1.1
 */
public interface TargetListener<T> {

	void targetCreated(Execution e, Map<String, Object> source, T target);

	void targetUpdated(Execution e, Map<String, Object> source, T target);
}
