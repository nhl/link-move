package com.nhl.link.etl.transform;

import java.util.List;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.query.SelectQuery;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.runtime.transform.key.KeyMapAdapter;

public class AttributeMatcher<T extends DataObject> extends BaseMatcher<T> implements CayenneMatcher<T> {

	private final String keyAttribute;

	public AttributeMatcher(KeyMapAdapter keyBuilder, String keyAttribute) {
		super(keyBuilder);
		this.keyAttribute = keyAttribute;
	}

	@Override
	public void apply(SelectQuery<T> query, List<Map<String, Object>> sources) {
		query.andQualifier(ExpressionFactory.inExp(keyAttribute, getSourceKeys(sources)));
	}

	@Override
	protected Object getSourceKey(Map<String, Object> source) {
		Object key = source.get(keyAttribute);
		if (key == null) {
			throw new EtlRuntimeException("Null source key for " + keyAttribute);
		}
		return key;
	}

	@Override
	protected Object getTargetKey(T target) {
		return target.readProperty(keyAttribute);
	}
}
