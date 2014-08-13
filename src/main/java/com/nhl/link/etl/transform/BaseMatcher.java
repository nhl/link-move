package com.nhl.link.etl.transform;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.keybuilder.KeyBuilder;

public abstract class BaseMatcher<T> implements Matcher<T> {

	private Map<Object, T> targetsMap;
	private KeyBuilder keyBuilder;

	public BaseMatcher(KeyBuilder keyBuilder) {
		this.keyBuilder = keyBuilder;
	}

	protected Set<Object> getSourceKeys(List<Map<String, Object>> sources) {
		Set<Object> sourceKeys = new HashSet<>();
		for (Map<String, Object> source : sources) {
			Object key = getSourceKey(source);
			sourceKeys.add(key);
		}
		return sourceKeys;
	}

	protected abstract Object getSourceKey(Map<String, Object> source);

	protected abstract Object getTargetKey(T target);

	@Override
	public void setTargets(List<T> targets) {
		targetsMap = new HashMap<>(targets.size());
		for (T target : targets) {
			Object targetKey = getTargetKey(target);
			if (targetKey == null) {
				throw new EtlRuntimeException("Null target key");
			}
			targetsMap.put(keyBuilder.toKey(targetKey), target);
		}
	}

	@Override
	public T find(Map<String, Object> source) {
		Object sourceKey = getSourceKey(source);
		return targetsMap.get(keyBuilder.toKey(sourceKey));
	}
}
