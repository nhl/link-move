package com.nhl.link.etl.transform;

import java.util.List;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.query.SelectQuery;

public interface CayenneMatcher<T extends DataObject> extends Matcher<T> {

	void apply(SelectQuery<T> query, List<Map<String, Object>> sources);
}