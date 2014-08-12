package com.nhl.link.framework.etl.transform;

import com.nhl.link.framework.etl.EtlRuntimeException;
import com.nhl.link.framework.etl.keybuilder.KeyBuilder;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.query.SelectQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiAttributeMatcher<T extends DataObject> extends BaseMatcher<T> implements CayenneMatcher<T> {
	private final List<String> keyAttributes;

	public MultiAttributeMatcher(KeyBuilder keyBuilder, List<String> keyAttributes) {
		super(keyBuilder);
		this.keyAttributes = keyAttributes;
	}

	@Override
	protected Object getSourceKey(Map<String, Object> source) {
		List<Object> compoundSourceKey = new ArrayList<>();
		for (String keyAttribute : keyAttributes) {
			Object key = source.get(keyAttribute);
			if (key == null) {
				throw new EtlRuntimeException("Null source key for " + keyAttribute);
			}
			compoundSourceKey.add(key);
		}
		return compoundSourceKey;
	}

	@Override
	protected Object getTargetKey(T target) {
		List<Object> compoundTargetKey = new ArrayList<>();
		for (String keyAttribute : keyAttributes) {
			compoundTargetKey.add(target.readProperty(keyAttribute));
		}
		return compoundTargetKey;
	}

	@Override
	public void apply(SelectQuery<T> query, List<Map<String, Object>> sources) {
		Expression fullExpression = null;
		for (Map<String, Object> source : sources) {
			Expression compoundKeyExpression = null;
			for (String keyAttribute : keyAttributes) {
				Expression keyExpression = ExpressionFactory.matchExp(keyAttribute, source.get(keyAttribute));
				if (compoundKeyExpression == null) {
					compoundKeyExpression = keyExpression;
				} else {
					compoundKeyExpression = compoundKeyExpression.andExp(keyExpression);
				}
			}
			if (fullExpression == null) {
				fullExpression = compoundKeyExpression;
			} else {
				fullExpression = fullExpression.orExp(compoundKeyExpression);
			}
		}
		query.andQualifier(fullExpression);
	}
}
