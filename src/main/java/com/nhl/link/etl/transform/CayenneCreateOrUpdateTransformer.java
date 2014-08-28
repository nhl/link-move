package com.nhl.link.etl.transform;

import com.nhl.link.etl.Execution;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.query.SelectQuery;

import java.util.List;
import java.util.Map;

public class CayenneCreateOrUpdateTransformer<T extends DataObject> extends CreateOrUpdateTransformer<T> {

	protected final ObjectContext context;
	protected final Execution execution;
	protected final CayenneMatcher<T> targetMatcher;
	protected final CayenneCreateOrUpdateStrategy<T> createOrUpdateStrategy;

	public CayenneCreateOrUpdateTransformer(Class<T> type, Execution execution,
	                                        CayenneMatcher<T> cayenneMatcher,
	                                        CayenneCreateOrUpdateStrategy<T> createOrUpdateStrategy,
	                                        List<TransformListener<T>> transformListeners,
	                                        List<TransformFilter> transformFilters,
	                                        ObjectContext context) {
		super(type, cayenneMatcher, transformListeners, transformFilters);
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
	protected List<T> getTargets(List<Map<String, Object>> sources) {
		SelectQuery<T> select = SelectQuery.query(type);
		targetMatcher.apply(select, sources);
		return context.select(select);
	}
}
