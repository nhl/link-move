package com.nhl.link.etl.transform;

import java.util.Map;

public interface TransformListener<T> {

	void targetCreated(Map<String, Object> source, T target);

	void targetUpdated(Map<String, Object> source, T target);
}
