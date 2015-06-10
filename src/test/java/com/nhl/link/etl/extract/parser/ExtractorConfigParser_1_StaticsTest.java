package com.nhl.link.etl.extract.parser;

import static org.junit.Assert.assertSame;

import org.junit.Test;

public class ExtractorConfigParser_1_StaticsTest {

	@Test
	public void testGetType() throws ClassNotFoundException {

		assertSame(String.class, ExtractorConfigParser_1.getType("java.lang.String"));
		assertSame(Integer.class, ExtractorConfigParser_1.getType("java.lang.Integer"));
		assertSame(byte[].class, ExtractorConfigParser_1.getType("byte[]"));
		assertSame(Integer.TYPE, ExtractorConfigParser_1.getType("int"));
	}
}
