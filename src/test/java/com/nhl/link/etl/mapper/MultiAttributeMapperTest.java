package com.nhl.link.etl.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.junit.Test;

public class MultiAttributeMapperTest {

	@Test
	public void testExpressionForKey() {

		MultiAttributeMapper mapper = new MultiAttributeMapper(Arrays.asList("a", "b"));
		Map<String, Object> key = new HashMap<>();
		key.put("a", "a1");
		key.put("b", 5);

		Expression e = mapper.expressionForKey(key);
		assertEquals(ExpressionFactory.exp("a = 'a1' and b = 5"), e);
	}

	@Test
	public void testKeyForSource() {
		MultiAttributeMapper mapper = new MultiAttributeMapper(Arrays.asList("a", "b"));

		Map<String, Object> source = new HashMap<>();
		source.put("a", "a1");
		source.put("b", 5);
		source.put("c", 6);

		Object key = mapper.keyForSource(source);
		assertTrue(key instanceof Map);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		Map<String, Object> keyMap = (Map) key;
		assertEquals(2, keyMap.size());
		assertEquals("a1", keyMap.get("a"));
		assertEquals(5, keyMap.get("b"));
	}
	
	@Test
	public void testKeyForTarget() {
		MultiAttributeMapper mapper = new MultiAttributeMapper(Arrays.asList("a", "b"));

		DataObject target = mock(DataObject.class);
		when(target.readProperty("a")).thenReturn("a1");
		when(target.readProperty("b")).thenReturn(5);
		when(target.readProperty("c")).thenReturn(6);
		
		Object key = mapper.keyForTarget(target);
		assertTrue(key instanceof Map);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		Map<String, Object> keyMap = (Map) key;
		assertEquals(2, keyMap.size());
		assertEquals("a1", keyMap.get("a"));
		assertEquals(5, keyMap.get("b"));
	}
}
