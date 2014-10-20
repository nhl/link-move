package com.nhl.link.etl.load.matcher;

import java.util.Map;

import org.apache.cayenne.exp.Expression;

/**
 * A strategy object for calculating a "key" from source and target objects of the ETL. The key is then used for 
 */
public interface Matcher<T> {

	Object keyForTarget(T target);

	Object keyForSource(Map<String, Object> source);

	Expression expressionForKey(Object key);
}
