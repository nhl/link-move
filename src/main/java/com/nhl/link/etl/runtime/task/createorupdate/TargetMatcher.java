package com.nhl.link.etl.runtime.task.createorupdate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.query.ObjectSelect;

import com.nhl.link.etl.mapper.Mapper;

/**
 * @since 1.3
 */
public class TargetMatcher<T> {

	private Class<T> type;
	private Mapper mapper;

	public TargetMatcher(Class<T> type, Mapper mapper) {
		this.type = type;
		this.mapper = mapper;
	}

	public List<T> match(ObjectContext context, Map<Object, Map<String, Object>> mappedSegment) {

		Collection<Object> keys = mappedSegment.keySet();

		List<Expression> expressions = new ArrayList<>(keys.size());
		for (Object key : keys) {

			Expression e = mapper.expressionForKey(key);
			if (e != null) {
				expressions.add(e);
			}
		}

		// no keys (?)
		if (expressions.isEmpty()) {
			return Collections.emptyList();
		} else {
			return ObjectSelect.query(type).where(ExpressionFactory.or(expressions)).select(context);
		}
	}
}
