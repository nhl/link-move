package com.nhl.link.framework.etl.transform;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.parser.ASTObjPath;
import org.apache.cayenne.query.SelectQuery;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AttributeMatcherTest extends CayenneMatcherTest {
	private AttributeMatcher<DataObject> matcher;

	private List<DataObject> targets;

	@Before
	public void setUpMatcher() {
		matcher = new AttributeMatcher<>(keyBuilderMock, SOURCE_KEY);
	}

	@Before
	public void setUpTargets() {
		targets = new ArrayList<>();
		for (final Map<String, Object> source : sources) {
			DataObject target = mock(DataObject.class);
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

	@Override
	protected BaseMatcher<DataObject> getMatcher() {
		return matcher;
	}

	@Override
	protected List<DataObject> getTargets() {
		return targets;
	}

	@Override
	protected void verifyGetTargetKey(DataObject target) {
		verify(target, atLeastOnce()).readProperty(anyString());
	}

	@Test
	public void testApply() {
		SelectQuery<DataObject> query = new SelectQuery<>();
		matcher.apply(query, sources);

		checkInExpression(query, ASTObjPath.class);
	}
}
