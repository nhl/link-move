package com.nhl.link.etl.mapper;

import static org.apache.cayenne.exp.ExpressionFactory.matchDbExp;

import java.util.Map;

import org.apache.cayenne.Cayenne;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Expression;

/**
 * @since 1.1
 */
public class IdMapper implements Mapper {

	private String targetIdColumn;
	private final String sourceIdName;

	public IdMapper(String targetIdColumn, String sourceIdName) {
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
	public Object keyForTarget(DataObject target) {
		return Cayenne.pkForObject(target);
	}

}
