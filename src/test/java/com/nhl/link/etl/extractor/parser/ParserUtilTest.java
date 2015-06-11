package com.nhl.link.etl.extractor.parser;

import static org.junit.Assert.assertSame;

import org.junit.Test;

public class ParserUtilTest {

	@Test
	public void testTypeForName() throws ClassNotFoundException {

		assertSame(String.class, ParserUtil.typeForName("java.lang.String"));
		assertSame(Integer.class, ParserUtil.typeForName("java.lang.Integer"));
		assertSame(byte[].class, ParserUtil.typeForName("byte[]"));
		assertSame(Integer.TYPE, ParserUtil.typeForName("int"));
	}
}
