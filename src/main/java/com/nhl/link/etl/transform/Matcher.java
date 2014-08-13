package com.nhl.link.etl.transform;

import java.util.List;
import java.util.Map;

public interface Matcher<T> {
	void setTargets(List<T> targets);

	T find(Map<String, Object> source);
}
