package com.nhl.link.move.mapper;

import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;

import com.nhl.link.move.EtlRuntimeException;

public class PathMapper implements Mapper {

	private String dbPath;

	// created lazily and cached... parsing expressions is expensive
	private Expression pathExpression;
	private Expression keyValueExpression;

	public PathMapper(String dbPath) {
		this.dbPath = dbPath;
	}

	private Expression getOrCreatePathExpression() {
		if (pathExpression == null) {
			// as we expect both Db and Obj paths here, let's pass the path
			// through the parser to generate the correct expression template...
			this.pathExpression = ExpressionFactory.exp(dbPath);
		}

		return pathExpression;
	}

	private Expression getOrCreateKeyValueExpression() {
		if (keyValueExpression == null) {
			// as we expect both Db and Obj paths here, let's pass the path
			// through the parser to generate the correct expression template...
			this.keyValueExpression = ExpressionFactory.exp(dbPath + " = $v");
		}

		return keyValueExpression;
	}

	@Override
	public Expression expressionForKey(Object key) {
		return getOrCreateKeyValueExpression().paramsArray(key);
	}

	@Override
	public Object keyForSource(Map<String, Object> source) {

		// if source does not contain a key, we must fail, otherwise multiple
		// rows will be incorrectly matched against NULL key

		Object key = source.get(dbPath);
		if (key == null && !source.containsKey(dbPath)) {
			throw new EtlRuntimeException("Source does not contain key path: " + dbPath);
		}

		return key;
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
		return getClass().getSimpleName() + "__" + dbPath;
	}
}
