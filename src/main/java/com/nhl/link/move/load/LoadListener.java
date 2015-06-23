package com.nhl.link.move.load;

import java.util.Map;

import com.nhl.link.move.Execution;

/**
 * A listener for target merge events.
 * 
 * @since 1.1
 * @deprecated since 1.3 use one of the stage listeners, e.g.
 * @AfterTargetMatched.
 */
@Deprecated
public interface LoadListener<T> {

	void targetCreated(Execution e, Map<String, Object> source, T target);

	void targetUpdated(Execution e, Map<String, Object> source, T target);
}
