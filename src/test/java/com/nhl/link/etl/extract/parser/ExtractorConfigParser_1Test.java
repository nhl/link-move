package com.nhl.link.etl.extract.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;

import com.nhl.link.etl.extract.ExtractorConfig;

public class ExtractorConfigParser_1Test extends BaseParserTest {

	private ExtractorConfigParser_1 parser;

	@Before
	public void before() {
		this.parser = new ExtractorConfigParser_1();
	}

	@Test
	public void testParse() {

		Element rootElement = getXmlRoot("extractor_v1.xml");
		ExtractorConfig config = new ExtractorConfig("aname");

		parser.parse(rootElement, config);

		assertNotNull(config);
		assertEquals("atype", config.getType());
		assertEquals("aconnector", config.getConnectorId());
		assertEquals(3, config.getAttributes().length);

		assertEquals(0, config.getAttributes()[0].getOrdinal());
		assertEquals(String.class, config.getAttributes()[0].type());
		assertEquals("a1", config.getAttributes()[0].getSourceName());
		assertEquals("a_1", config.getAttributes()[0].getTargetPath());

		assertEquals(1, config.getAttributes()[1].getOrdinal());
		assertEquals(Integer.class, config.getAttributes()[1].type());
		assertEquals("a2", config.getAttributes()[1].getSourceName());
		assertEquals("a_2", config.getAttributes()[1].getTargetPath());

		assertEquals(2, config.getAttributes()[2].getOrdinal());
		assertEquals(Integer.class, config.getAttributes()[2].type());
		assertEquals("a2", config.getAttributes()[2].getSourceName());
		assertNull(config.getAttributes()[2].getTargetPath());

		assertEquals(2, config.getProperties().size());
		assertEquals("AB", config.getProperties().get("a.b"));
		assertEquals("XY", config.getProperties().get("x.y"));
	}
}
