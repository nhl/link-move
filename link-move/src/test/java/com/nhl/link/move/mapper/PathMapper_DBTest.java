package com.nhl.link.move.mapper;

import static org.junit.Assert.assertEquals;

import org.apache.cayenne.exp.ExpressionFactory;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.move.mapper.PathMapper;

public class PathMapper_DBTest {

	private PathMapper mapper;

	@Before
	public void before() {
		mapper = new PathMapper("db:abc");
	}

	@Test
	public void testExpressionForKey() {
		assertEquals(ExpressionFactory.exp("db:abc = \"a\""), mapper.expressionForKey("a"));
	}
}
