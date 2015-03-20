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

	private Mapper<T> mapper;

	public TargetMatcher(Mapper<T> mapper) {
		this.mapper = mapper;
	}

	public List<T> match(Class<T> type, ObjectContext context, Map<Object, Map<String, Object>> mappedSegment) {

		Collection<Object> keys = mappedSegment.keySet();

		// TODO: split query in batches:
		// respect Constants.SERVER_MAX_ID_QUALIFIER_SIZE_PROPERTY
		// property of Cayenne , breaking query into subqueries.
		// Otherwise this operation will not scale.. Though I guess since we are
		// not using streaming API to read data from Cayenne, we are already
		// limited in how much data can fit in the memory map.

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
