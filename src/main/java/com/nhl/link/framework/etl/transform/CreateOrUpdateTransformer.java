package com.nhl.link.framework.etl.transform;

import com.nhl.link.framework.etl.batch.BatchProcessor;

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

	protected CreateOrUpdateTransformer(Class<T> type, Matcher<T> matcher) {
		this(type, matcher, Collections.<TransformListener<T>>emptyList());
	}

	/**
	 * @since 6.16
	 */
	public CreateOrUpdateTransformer(Class<T> type, Matcher<T> matcher, List<TransformListener<T>> transformListeners) {
		this.type = type;
		this.matcher = matcher;
		this.transformListeners = transformListeners;
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
			T target = matcher.find(source);
			if (target != null) {
				update(source, target);
				fireTargetUpdated(source, target);
			} else {
				target = create(source);
				fireTargetCreated(source, target);
			}
		}
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
