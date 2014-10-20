package com.nhl.link.etl.load.matcher;

import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;

public class AttributeMatcher<T extends DataObject> implements Matcher<T> {

	private final String keyProperty;

	public AttributeMatcher(String keyProperty) {
		this.keyProperty = keyProperty;
	}

	@Override
	public Expression expressionForKey(Object key) {
		// allowing nulls here
		return ExpressionFactory.matchExp(keyProperty, key);
	}

	@Override
	public Object keyForSource(Map<String, Object> source) {
		return source.get(keyProperty);
	}

	@Override
	public Object keyForTarget(T target) {
		return target.readProperty(keyProperty);
	}
}
