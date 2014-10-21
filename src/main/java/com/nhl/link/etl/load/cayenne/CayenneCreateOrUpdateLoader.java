package com.nhl.link.etl.load.cayenne;

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
import com.nhl.link.etl.load.CreateOrUpdateLoader;
import com.nhl.link.etl.load.LoadListener;
import com.nhl.link.etl.load.mapper.Mapper;

public class CayenneCreateOrUpdateLoader<T extends DataObject> extends CreateOrUpdateLoader<T> {

	protected final ObjectContext context;
	protected final Execution execution;
	protected final CayenneCreateOrUpdateStrategy<T> createOrUpdateStrategy;

	public CayenneCreateOrUpdateLoader(Class<T> type, Execution execution, Mapper<T> mapper,
			CayenneCreateOrUpdateStrategy<T> createOrUpdateStrategy, List<LoadListener<T>> transformListeners,
			ObjectContext context) {

		super(type, mapper, transformListeners);
		this.context = context;
		this.execution = execution;
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

			Expression e = mapper.expressionForKey(key);
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
