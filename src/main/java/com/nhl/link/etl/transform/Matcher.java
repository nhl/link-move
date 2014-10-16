package com.nhl.link.etl.transform;

import java.util.List;
import java.util.Map;

import org.apache.cayenne.query.SelectQuery;

public interface Matcher<T> {

	void setTargets(List<T> targets);

	T find(Map<String, Object> source);

	void apply(SelectQuery<T> query, List<Map<String, Object>> sources);
}
