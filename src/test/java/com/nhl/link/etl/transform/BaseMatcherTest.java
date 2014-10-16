package com.nhl.link.etl.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.parser.ASTIn;
import org.apache.cayenne.exp.parser.ASTList;
import org.apache.cayenne.exp.parser.ASTPath;
import org.apache.cayenne.query.SelectQuery;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.nhl.link.etl.map.key.KeyMapAdapter;

public abstract class BaseMatcherTest {
	protected List<Map<String, Object>> sources;

	protected KeyMapAdapter keyBuilderMock;

	protected static final String SOURCE_KEY = "attr1";

	protected static final String SOURCE_ATTRIBUTE_PREFIX = "attr";

	protected static final String SOURCE_VALUE_PREFIX = "value";

	@Before
	public void setUpSources() {
		sources = new ArrayList<>();

		for (int i = 0; i <= 9; i++) {
			Map<String, Object> source = new HashMap<>();
			sources.add(source);
			for (int j = 0; j <= 2; j++) {
				source.put(SOURCE_ATTRIBUTE_PREFIX + j, SOURCE_VALUE_PREFIX + j + i);
			}
		}
	}

	@Before
	public void setUpKeyBuilderMock() {
		keyBuilderMock = mock(KeyMapAdapter.class);
		when(keyBuilderMock.toMapKey(anyObject())).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				return invocation.getArguments()[0];
			}
		});
	}

	protected abstract BaseMatcher<DataObject> getMatcher();

	protected abstract List<DataObject> getTargets();

	protected abstract void verifyGetTargetKey(DataObject t);

	@Test
	public void testFind() {
		Map<String, Object> source = sources.get(0);
		BaseMatcher<DataObject> matcher = getMatcher();
		List<DataObject> targets = getTargets();

		matcher.setTargets(targets);
		DataObject target = matcher.find(source);
		for (String attr : source.keySet()) {
			assertEquals(source.get(attr), target.readProperty(attr));
		}
		verify(keyBuilderMock, times(targets.size() + 1)).toMapKey(anyObject());
		for (DataObject t : targets) {
			verifyGetTargetKey(t);
		}
	}
	
	protected void checkInExpression(SelectQuery<DataObject> query, Class<? extends ASTPath> pathClass) {
		assertEquals(ASTIn.class, query.getQualifier().getClass());

		ASTIn expression = (ASTIn) query.getQualifier();
		assertNotNull(expression.jjtGetChild(0));
		assertEquals(pathClass, expression.jjtGetChild(0).getClass());
		assertEquals(SOURCE_KEY, ((ASTPath) expression.jjtGetChild(0)).getPath());
		assertNotNull(expression.jjtGetChild(1));
		assertEquals(ASTList.class, expression.jjtGetChild(1).getClass());
		Object[] values = (Object[]) ((ASTList) expression.jjtGetChild(1)).evaluate(null);

		Set<Object> sourceValues = new HashSet<>();
		for (Map<String, Object> source : sources) {
			sourceValues.add(source.get(SOURCE_KEY));
		}
		assertEquals(sourceValues.size(), values.length);
		for (Object value : values) {
			assertTrue(sourceValues.contains(value));
		}
	}

}
