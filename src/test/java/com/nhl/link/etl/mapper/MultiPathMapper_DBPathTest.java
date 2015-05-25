package com.nhl.link.etl.mapper;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.junit.Before;
import org.junit.Test;

public class MultiPathMapper_DBPathTest {

	private MultiPathMapper mapper;

	@Before
	public void before() {

		// must ensure predictable key iteration order, so using LinkedMap
		Map<String, Mapper> mappers = new LinkedHashMap<>();
		mappers.put("db:a", new PathMapper("db:a"));
		mappers.put("db:b", new PathMapper("db:b"));

		mapper = new MultiPathMapper(mappers);
	}

	@Test
	public void testExpressionForKey() {

		Map<String, Object> key = new HashMap<>();
		key.put("db:a", "a1");
		key.put("db:b", 5);

		Expression e = mapper.expressionForKey(key);
		assertEquals(ExpressionFactory.exp("db:a = 'a1' and db:b = 5"), e);
	}
}
