package com.nhl.link.etl.load.mapper;

import static org.apache.cayenne.exp.ExpressionFactory.joinExp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;

public class MultiAttributeMapper<T extends DataObject> implements Mapper<T> {

	private final List<String> keyProperties;

	public MultiAttributeMapper(List<String> keyProperties) {
		this.keyProperties = keyProperties;
	}

	@Override
	public Expression expressionForKey(Object key) {

		@SuppressWarnings("unchecked")
		Map<String, Object> keyMap = (Map<String, Object>) key;

		List<Expression> clauses = new ArrayList<>(keyProperties.size());

		for (String property : keyProperties) {
			Object value = keyMap.get(property);
			clauses.add(ExpressionFactory.matchExp(property, value));
		}

		return joinExp(Expression.AND, clauses);
	}

	@Override
	public Object keyForSource(Map<String, Object> source) {

		Map<String, Object> keyMap = new HashMap<String, Object>(keyProperties.size() * 2);
		for (String property : keyProperties) {
			// null keys are ok (I guess?)
			keyMap.put(property, source.get(property));
		}

		return keyMap;
	}

	@Override
	public Object keyForTarget(T target) {
		Map<String, Object> keyMap = new HashMap<String, Object>(keyProperties.size() * 2);

		for (String property : keyProperties) {
			// null keys are ok (I guess?)
			keyMap.put(property, target.readProperty(property));
		}
		return keyMap;
	}
}
