package com.nhl.link.etl.transform;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectId;
import org.apache.cayenne.exp.parser.ASTDbPath;
import org.apache.cayenne.query.SelectQuery;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class IdMatcherTest extends BaseMatcherTest {

	private IdMatcher<DataObject> matcher;
	private List<DataObject> targets;

	@Before
	public void setUpMatcher() {
		matcher = new IdMatcher<>(keyMapAdapterMock, SOURCE_KEY);
	}

	@Before
	public void setUpTargets() {
		targets = new ArrayList<>();
		for (final Map<String, Object> source : sources) {
			DataObject target = mock(DataObject.class);
			when(target.getObjectId()).thenReturn(new ObjectId(null, SOURCE_KEY, source.get(SOURCE_KEY)));
			when(target.readProperty(anyString())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					String attr = (String) invocation.getArguments()[0];
					return source.get(attr);
				}
			});
			targets.add(target);
		}
	}

	@Test
	public void testFind() {
		Map<String, Object> source = sources.get(0);

		matcher.setTargets(targets);
		DataObject target = matcher.find(source);
		for (String attr : source.keySet()) {
			assertEquals(source.get(attr), target.readProperty(attr));
		}
		verify(keyMapAdapterMock, times(targets.size() + 1)).toMapKey(anyObject());
		for (DataObject t : targets) {
			verify(t).getObjectId();
		}
	}

	@Test
	public void testApply() {
		SelectQuery<DataObject> query = new SelectQuery<>();
		matcher.apply(query, sources);

		checkInExpression(query, ASTDbPath.class);
	}
}
