package com.nhl.link.etl.transform;

import java.util.List;
import java.util.Map;

import org.apache.cayenne.Cayenne;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.query.SelectQuery;

import com.nhl.link.etl.map.key.KeyMapAdapter;

/**
 * @since 1.1
 */
public class IdMatcher<T extends DataObject> extends BaseMatcher<T> implements Matcher<T> {
	private final String pkAttribute;

	public IdMatcher(KeyMapAdapter keyBuilder, String primaryKeyAttribute) {
		super(keyBuilder);
		this.pkAttribute = primaryKeyAttribute;
	}

	@Override
	public void apply(SelectQuery<T> query, List<Map<String, Object>> sources) {
		query.andQualifier(ExpressionFactory.inDbExp(pkAttribute, getSourceKeys(sources)));
	}

	@Override
	protected Object getSourceKey(Map<String, Object> source) {
		return source.get(pkAttribute);
	}

	@Override
	protected Object getTargetKey(T target) {
		return Cayenne.pkForObject(target);
	}
}
