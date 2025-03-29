package com.nhl.link.move.mapper;

import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiPathMapper_DBPathTest {

	private MultiPathMapper mapper;

	@BeforeEach
	public void before() {

		// must ensure predictable key iteration order, so using LinkedMap
		Map<String, Mapper> mappers = new LinkedHashMap<>();
		mappers.put("db:a", new PathMapper("db:a"));
		mappers.put("db:b", new PathMapper("db:b"));

		mapper = new MultiPathMapper(mappers);
	}

	@Test
	public void expressionForKey() {

		Map<String, Object> key = new HashMap<>();
		key.put("db:a", "a1");
		key.put("db:b", 5);

		Expression e = mapper.expressionForKey(key);
		assertEquals(ExpressionFactory.exp("db:a = 'a1' and db:b = 5"), e);
	}
}
