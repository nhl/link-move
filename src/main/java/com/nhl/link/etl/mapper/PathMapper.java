package com.nhl.link.etl.mapper;

import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;

public class PathMapper implements Mapper {

	private String path;

	// created lazily and cached... parsing expressions is expensive
	private Expression pathExpression;
	private Expression keyValueExpression;

	public PathMapper(String path) {
		this.path = path;
	}

	private Expression getOrCreatePathExpression() {
		if (pathExpression == null) {
			// as we expect both Db and Obj paths here, let's pass the path
			// through the parser to generate the correct expression template...
			this.pathExpression = ExpressionFactory.exp(path);
		}

		return pathExpression;
	}

	private Expression getOrCreateKeyValueExpression() {
		if (keyValueExpression == null) {
			// as we expect both Db and Obj paths here, let's pass the path
			// through the parser to generate the correct expression template...
			this.keyValueExpression = ExpressionFactory.exp(path + " = $v");
		}

		return keyValueExpression;
	}

	@Override
	public Expression expressionForKey(Object key) {
		return getOrCreateKeyValueExpression().paramsArray(key);
	}

	@Override
	public Object keyForSource(Map<String, Object> source) {
		return source.get(path);
	}

	@Override
	public Object keyForTarget(DataObject target) {

		// cases:
		// 1. "obj:" expressions are object properties
		// 2. "db:" expressions mapping to ID columns
		// 3. "db:" expressions mapping to object properties

		// Cayenne exp can handle 1 & 2; we'll need to manually handle case 3

		return getOrCreatePathExpression().evaluate(target);
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName() + "__" + path;
	}
}
