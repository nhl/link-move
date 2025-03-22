package com.nhl.link.move.mapper;

import org.dflib.Index;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiPathMapperTest {

	private MultiPathMapper mapper;

	@BeforeEach
	public void before() {

		// must ensure predictable key iteration order, so using LinkedMap
		Map<String, Mapper> mappers = new LinkedHashMap<>();
		mappers.put("a", new PathMapper("a"));
		mappers.put("b", new PathMapper("b"));

		mapper = new MultiPathMapper(mappers);
	}

	@Test
	public void testExpressionForKey() {

		Map<String, Object> key = new HashMap<>();
		key.put("a", "a1");
		key.put("b", 5);

		Expression e = mapper.expressionForKey(key);
		assertEquals(ExpressionFactory.exp("a = 'a1' and b = 5"), e);
	}

	@Test
	public void testKeyForSource() {

		Map<String, Object> source = new HashMap<>();
		source.put("a", "a1");
		source.put("b", 5);
		source.put("c", 6);

		Object key = mapper.keyForSource(new TestRowProxy(Index.of("a", "b", "c"), "a1", 5, 6));
		assertTrue(key instanceof Map);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		Map<String, Object> keyMap = (Map) key;
		assertEquals(2, keyMap.size());
		assertEquals("a1", keyMap.get("a"));
		assertEquals(5, keyMap.get("b"));
	}

	@Test
	public void testKeyForTarget() {

		DataObject target = mock(DataObject.class);
		when(target.readNestedProperty("a")).thenReturn("a1");
		when(target.readNestedProperty("b")).thenReturn(5);
		when(target.readNestedProperty("c")).thenReturn(6);

		Object key = mapper.keyForTarget(target);
		assertTrue(key instanceof Map);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		Map<String, Object> keyMap = (Map) key;
		assertEquals(2, keyMap.size());
		assertEquals("a1", keyMap.get("a"));
		assertEquals(5, keyMap.get("b"));
	}
}
