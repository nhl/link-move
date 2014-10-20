package com.nhl.link.etl.load.matcher;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectId;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.etl.load.matcher.IdMatcher;

public class IdMatcherTest {

	private IdMatcher<DataObject> matcher;

	@Before
	public void setUpMatcher() {
		matcher = new IdMatcher<>("TID", "SID");
	}

	@Test
	public void testKeyForSource() {

		Map<String, Object> src = new HashMap<String, Object>();
		src.put("SID", 34);
		src.put("abc", "ABC");

		assertEquals(34, matcher.keyForSource(src));
	}

	@Test
	public void testKeyForTarget() {

		ObjectId id = new ObjectId("dummy", "TID", 55);
		DataObject t = mock(DataObject.class);
		when(t.getObjectId()).thenReturn(id);

		assertEquals(55, matcher.keyForTarget(t));
	}

	@Test
	public void testExpressionForKey() {
		assertEquals("db:TID = 55", matcher.expressionForKey(55).toString());
	}
}
