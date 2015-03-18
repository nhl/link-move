package com.nhl.link.etl.load;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.batch.BatchProcessor;
import com.nhl.link.etl.load.mapper.Mapper;

/**
 * A stateful processor that matches a list of source data maps with target
 * Cayenne objects with a provided matcher. Targets are created if missing or
 * updated if present.
 */
public abstract class CreateOrUpdateLoader<T> implements BatchProcessor<Map<String, Object>> {

	protected final Class<T> type;
	protected final Execution execution;
	protected final Mapper<T> mapper;
	protected final List<LoadListener<T>> transformListeners;

	public CreateOrUpdateLoader(Class<T> type, Mapper<T> mapper, Execution execution,
			List<LoadListener<T>> transformListeners) {
		this.type = type;
		this.mapper = mapper;
		this.execution = execution;
		this.transformListeners = transformListeners;
	}

	protected abstract T create(Map<String, Object> source);

	protected abstract void update(Map<String, Object> source, T target);

	protected abstract List<T> getTargets(Collection<Object> keys);

	@Override
	public void process(List<Map<String, Object>> segment) {

		Map<Object, Map<String, Object>> mutableSrcMap = mutableSrcMap(segment);
		List<T> targets = getTargets(mutableSrcMap.keySet());
		for (T t : targets) {

			Object key = mapper.keyForTarget(t);

			Map<String, Object> src = mutableSrcMap.remove(key);

			// a null can only mean some algorithm malfunction, as keys are all
			// coming from a known set of sources
			if (src == null) {
				throw new EtlRuntimeException("Invalid key: " + key);
			}

			update(src, t);
			fireTargetUpdated(src, t);
		}

		// everything that's left are new objects
		for (Entry<Object, Map<String, Object>> e : mutableSrcMap.entrySet()) {

			T t = create(e.getValue());
			fireTargetCreated(e.getValue(), t);
		}
	}

	private Map<Object, Map<String, Object>> mutableSrcMap(List<Map<String, Object>> segment) {
		Map<Object, Map<String, Object>> byKey = new HashMap<>();

		for (Map<String, Object> s : segment) {
			// TODO: report dupes?
			byKey.put(mapper.keyForSource(s), s);
		}

		return byKey;
	}

	protected void fireTargetCreated(Map<String, Object> source, T target) {
		for (LoadListener<T> listener : transformListeners) {
			listener.targetCreated(execution, source, target);
		}
	}

	protected void fireTargetUpdated(Map<String, Object> source, T target) {
		for (LoadListener<T> listener : transformListeners) {
			listener.targetUpdated(execution, source, target);
		}
	}
}
