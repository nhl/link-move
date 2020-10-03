package com.nhl.link.move.mapper;

import org.apache.cayenne.exp.ExpressionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PathMapper_DBTest {

	private PathMapper mapper;

	@BeforeEach
	public void before() {
		mapper = new PathMapper("db:abc");
	}

	@Test
	public void testExpressionForKey() {
		assertEquals(ExpressionFactory.exp("db:abc = \"a\""), mapper.expressionForKey("a"));
	}
}
