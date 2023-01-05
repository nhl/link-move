package com.nhl.link.move.mapper;

import com.nhl.dflib.row.RowProxy;
import org.apache.cayenne.exp.Expression;

/**
 * A mapper that does transparent conversion between object key value and a map-friendly key.
 * 
 * @since 1.1
 */
public class SafeMapKeyMapper implements Mapper {

	private final Mapper delegate;
	private final KeyAdapter keyAdapter;

	public SafeMapKeyMapper(Mapper delegate, KeyAdapter keyAdapter) {
		this.delegate = delegate;
		this.keyAdapter = keyAdapter;
	}

	@Override
	public Object keyForTarget(Object target) {
		return keyAdapter.toMapKey(delegate.keyForTarget(target));
	}

	@Override
	public Object keyForSource(RowProxy source) {
		return keyAdapter.toMapKey(delegate.keyForSource(source));
	}

	@Override
	public Expression expressionForKey(Object key) {
		Object safeKey = keyAdapter.fromMapKey(key);
		return delegate.expressionForKey(safeKey);
	}
}
