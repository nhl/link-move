package com.nhl.link.etl.transform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.query.SelectQuery;

import com.nhl.link.etl.Execution;

public class CayenneCreateOrUpdateTransformer<T extends DataObject> extends CreateOrUpdateTransformer<T> {

	protected final ObjectContext context;
	protected final Execution execution;
	protected final Matcher<T> targetMatcher;
	protected final CayenneCreateOrUpdateStrategy<T> createOrUpdateStrategy;

	public CayenneCreateOrUpdateTransformer(Class<T> type, Execution execution, Matcher<T> cayenneMatcher,
			CayenneCreateOrUpdateStrategy<T> createOrUpdateStrategy, List<TransformListener<T>> transformListeners,
			ObjectContext context) {
		super(type, cayenneMatcher, transformListeners);
		this.context = context;
		this.execution = execution;
		this.targetMatcher = cayenneMatcher;
		this.createOrUpdateStrategy = createOrUpdateStrategy;
	}

	@Override
	public void process(List<Map<String, Object>> segment) {
		super.process(segment);

		// store counts before commit; update the execution after commit
		int created = context.newObjects().size();
		int updated = context.modifiedObjects().size();

		context.commitChanges();

		execution.incrementCreated(created);
		execution.incrementUpdated(updated);
	}

	@Override
	protected void update(Map<String, Object> source, T target) {
		createOrUpdateStrategy.update(context, source, target);
	}

	@Override
	protected T create(Map<String, Object> source) {
		return createOrUpdateStrategy.create(context, type, source);
	}

	@Override
	protected List<T> getTargets(Collection<Object> keys) {

		// TODO: split query in batches:
		// respect Constants.SERVER_MAX_ID_QUALIFIER_SIZE_PROPERTY
		// property of Cayenne , breaking query into subqueries.
		// Otherwise this operation will not scale.. Though I guess since we are
		// not using streaming API to read data from Cayenne, we are already
		// limited in how much data can fit in the memory map.

		List<Expression> expressions = new ArrayList<>(keys.size());
		for (Object key : keys) {

			Expression e = matcher.expressionForKey(key);
			if (e != null) {
				expressions.add(e);
			}
		}

		// no keys (?)
		if (expressions.isEmpty()) {
			return Collections.emptyList();
		}

		SelectQuery<T> query = SelectQuery.query(type);
		query.setQualifier(ExpressionFactory.joinExp(Expression.OR, expressions));
		return context.select(query);
	}
}
