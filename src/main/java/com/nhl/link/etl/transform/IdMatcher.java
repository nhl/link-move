package com.nhl.link.etl.transform;

import static org.apache.cayenne.exp.ExpressionFactory.matchDbExp;

import java.util.Map;

import org.apache.cayenne.Cayenne;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Expression;

/**
 * @since 1.1
 */
public class IdMatcher<T extends DataObject> implements Matcher<T> {

	private String targetIdColumn;
	private final String sourceIdName;

	public IdMatcher(String targetIdColumn, String sourceIdName) {
		this.targetIdColumn = targetIdColumn;
		this.sourceIdName = sourceIdName;
	}

	@Override
	public Expression expressionForKey(Object key) {
		return matchDbExp(targetIdColumn, key);
	}

	@Override
	public Object keyForSource(Map<String, Object> source) {
		return source.get(sourceIdName);
	}

	@Override
	public Object keyForTarget(T target) {
		return Cayenne.pkForObject(target);
	}

}
