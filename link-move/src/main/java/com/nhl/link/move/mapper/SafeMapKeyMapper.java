package com.nhl.link.move.mapper;

import com.nhl.yadf.Index;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Expression;

/**
 * A mapper that does transparent conversion between object key value and a map-friendly key.
 * 
 * @since 1.1
 */
public class SafeMapKeyMapper implements Mapper {

	private Mapper delegate;
	private KeyAdapter keyAdapter;

	public SafeMapKeyMapper(Mapper delegate, KeyAdapter keyAdapter) {
		this.delegate = delegate;
		this.keyAdapter = keyAdapter;
	}

	@Override
	public Object keyForTarget(DataObject target) {
		return keyAdapter.toMapKey(delegate.keyForTarget(target));
	}

	@Override
	public Object keyForSource(Index index, Object[] source) {
		return keyAdapter.toMapKey(delegate.keyForSource(index, source));
	}

	@Override
	public Expression expressionForKey(Object key) {
		key = keyAdapter.fromMapKey(key);
		return delegate.expressionForKey(key);
	}
}
