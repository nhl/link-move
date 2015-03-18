package com.nhl.link.etl.mapper;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.etl.mapper.AttributeMapper;

public class AttributeMapperTest {

	private AttributeMapper<DataObject> mapper;

	@Before
	public void setUpMatcher() {
		mapper = new AttributeMapper<>("abc");
	}

	@Test
	public void testKeyForSource() {

		Map<String, Object> src = new HashMap<String, Object>();
		src.put("a", "A");
		src.put("abc", "ABC");

		assertEquals("ABC", mapper.keyForSource(src));
	}

	@Test
	public void testKeyForTarget() {

		DataObject t = mock(DataObject.class);
		when(t.readProperty("abc")).thenReturn(44);

		assertEquals(44, mapper.keyForTarget(t));
	}

	@Test
	public void testExpressionForKey() {
		assertEquals("abc = \"a\"", mapper.expressionForKey("a").toString());
	}
}
