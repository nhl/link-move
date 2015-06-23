package com.nhl.link.move.mapper;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.move.EtlRuntimeException;
import com.nhl.link.move.mapper.PathMapper;

public class PathMapperTest {

	private PathMapper mapper;

	@Before
	public void before() {
		mapper = new PathMapper("abc");
	}

	@Test
	public void testKeyForSource() {

		Map<String, Object> src = new HashMap<String, Object>();
		src.put("a", "A");
		src.put("abc", "ABC");

		assertEquals("ABC", mapper.keyForSource(src));
	}

	@Test
	public void testKeyForSource_NullKey() {

		Map<String, Object> src = new HashMap<String, Object>();
		src.put("a", "A");
		src.put("abc", null);

		assertEquals(null, mapper.keyForSource(src));
	}

	@Test(expected = EtlRuntimeException.class)
	public void testKeyForSource_MissingKey() {

		Map<String, Object> src = new HashMap<String, Object>();
		src.put("a", "A");

		mapper.keyForSource(src);
	}

	@Test
	public void testKeyForTarget() {

		DataObject t = mock(DataObject.class);
		when(t.readProperty("abc")).thenReturn(44);
		when(t.readNestedProperty("abc")).thenReturn(44);

		assertEquals(44, mapper.keyForTarget(t));
	}

	@Test
	public void testExpressionForKey() {
		assertEquals("abc = \"a\"", mapper.expressionForKey("a").toString());
	}
}
