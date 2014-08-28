package com.nhl.link.etl.transform;

import java.util.Map;

/**
 * @since 1.1
 */
public interface TransformFilter {
	Map<String, Object> doFilter(Map<String, Object> source);
}
