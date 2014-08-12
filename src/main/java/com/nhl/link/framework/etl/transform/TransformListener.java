package com.nhl.link.framework.etl.transform;

import java.util.Map;

/**
 * @since 6.16
 */
public interface TransformListener<T> {
	public void targetCreated(Map<String, Object> source, T target);

	public void targetUpdated(Map<String, Object> source, T target);
}
