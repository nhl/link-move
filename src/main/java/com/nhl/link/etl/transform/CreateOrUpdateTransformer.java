package com.nhl.link.etl.transform;

import com.nhl.link.etl.batch.BatchProcessor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A stateful processor that matches a list of source data maps with target
 * Cayenne objects with a provided matcher. Targets are created if missing or updated
 * if present.
 */
public abstract class CreateOrUpdateTransformer<T> implements BatchProcessor<Map<String, Object>> {

	protected final Class<T> type;
	protected final Matcher<T> matcher;
	protected final List<TransformListener<T>> transformListeners;
	protected final List<TransformFilter> transformFilters;

	protected CreateOrUpdateTransformer(Class<T> type, Matcher<T> matcher) {
		this(type, matcher, Collections.<TransformListener<T>>emptyList(), Collections.<TransformFilter>emptyList());
	}

	public CreateOrUpdateTransformer(Class<T> type, Matcher<T> matcher, List<TransformListener<T>> transformListeners) {
		this(type, matcher, transformListeners, Collections.<TransformFilter>emptyList());
	}

	/**
	 * @since 1.1
	 */
	protected CreateOrUpdateTransformer(Class<T> type, Matcher<T> matcher, List<TransformListener<T>> transformListeners,
	                                    List<TransformFilter> transformFilters) {
		this.type = type;
		this.matcher = matcher;
		this.transformListeners = transformListeners;
		this.transformFilters = transformFilters;
	}

	protected abstract T create(Map<String, Object> source);

	protected abstract void update(Map<String, Object> source, T target);

	protected abstract List<T> getTargets(List<Map<String, Object>> source);

	@Override
	public void process(List<Map<String, Object>> segment) {
		matcher.setTargets(getTargets(segment));
		createOrUpdateTargets(segment);
	}

	protected void createOrUpdateTargets(List<Map<String, Object>> sources) {
		for (Map<String, Object> source : sources) {
			Map<String, Object> filteredSource = applyFilters(source);
			T target = matcher.find(filteredSource);
			if (target != null) {
				update(filteredSource, target);
				fireTargetUpdated(filteredSource, target);
			} else {
				target = create(filteredSource);
				fireTargetCreated(filteredSource, target);
			}
		}
	}

	private Map<String, Object> applyFilters(Map<String, Object> source) {
		for (TransformFilter filter : transformFilters) {
			source = filter.doFilter(source);
		}
		return source;
	}

	private void fireTargetCreated(Map<String, Object> source, T target) {
		for (TransformListener<T> listener : transformListeners) {
			listener.targetCreated(source, target);
		}
	}

	private void fireTargetUpdated(Map<String, Object> source, T target) {
		for (TransformListener<T> listener : transformListeners) {
			listener.targetUpdated(source, target);
		}
	}
}
