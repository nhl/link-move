package com.nhl.link.etl.load.matcher;

import java.util.Map;

import org.apache.cayenne.exp.Expression;

import com.nhl.link.etl.map.key.KeyMapAdapter;

/**
 * A matcher that does transparent conversion between object key value and a
 * map-friendly key.
 * 
 * @since 1.1
 */
public class SafeMapKeyMatcher<T> implements Matcher<T> {

	private Matcher<T> delegate;
	private KeyMapAdapter keyAdapter;

	public SafeMapKeyMatcher(Matcher<T> delegate, KeyMapAdapter keyAdapter) {
		this.delegate = delegate;
		this.keyAdapter = keyAdapter;
	}

	@Override
	public Object keyForTarget(T target) {
		return keyAdapter.toMapKey(delegate.keyForTarget(target));
	}

	@Override
	public Object keyForSource(Map<String, Object> source) {
		return keyAdapter.toMapKey(delegate.keyForSource(source));
	}

	@Override
	public Expression expressionForKey(Object key) {
		key = keyAdapter.fromMapKey(key);
		return delegate.expressionForKey(key);
	}
}
