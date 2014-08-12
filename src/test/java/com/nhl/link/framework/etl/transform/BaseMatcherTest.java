package com.nhl.link.framework.etl.transform;

import com.nhl.link.framework.etl.keybuilder.KeyBuilder;
import junit.framework.Assert;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.parser.ASTIn;
import org.apache.cayenne.exp.parser.ASTList;
import org.apache.cayenne.exp.parser.ASTObjPath;
import org.apache.cayenne.exp.parser.ASTPath;
import org.apache.cayenne.query.SelectQuery;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public abstract class BaseMatcherTest<T extends DataObject> {
	protected List<Map<String, Object>> sources;

	protected KeyBuilder keyBuilderMock;

	protected static final String SOURCE_KEY = "attr1";

	protected static final String SOURCE_ATTRIBUTE_PREFIX = "attr";

	protected static final String SOURCE_VALUE_PREFIX = "value";

	@Before
	public void setUpSources() {
		sources = new ArrayList<>();
		Map<String, Object> source;
		for (int i = 0; i <= 9; i++) {
			source = new HashMap<>();
			sources.add(source);
			for (int j = 0; j <= 2; j++) {
				source.put(SOURCE_ATTRIBUTE_PREFIX + j, SOURCE_VALUE_PREFIX + j + i);
			}
		}
	}

	@Before
	public void setUpKeyBuilderMock() {
		keyBuilderMock = mock(KeyBuilder.class);
		when(keyBuilderMock.toKey(anyObject())).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				return invocation.getArguments()[0];
			}
		});
	}

	protected abstract BaseMatcher<T> getMatcher();

	protected abstract List<T> getTargets();

	protected abstract void verifyGetTargetKey(T t);

	@Test
	public void testFind() {
		Map<String, Object> source = sources.get(0);
		BaseMatcher<T> matcher = getMatcher();
		List<T> targets = getTargets();

		matcher.setTargets(targets);
		T target = matcher.find(source);
		for (String attr : source.keySet()) {
			assertEquals(source.get(attr), target.readProperty(attr));
		}
		verify(keyBuilderMock, times(targets.size() + 1)).toKey(anyObject());
		for (T t : targets) {
			verifyGetTargetKey(t);
		}
	}

}
