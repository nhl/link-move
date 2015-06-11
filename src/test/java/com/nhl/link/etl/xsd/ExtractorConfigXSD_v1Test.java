package com.nhl.link.etl.xsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class ExtractorConfigXSD_v1Test extends BaseSchemaTest {

	private static final String SCHEMA_V1 = "extractor_config_1.xsd";

	@Test
	public void testValid() throws SAXException, IOException {
		validate("valid_extractor_v1.xml", SCHEMA_V1);
	}

	@Test
	public void testIncorrectOrdering() throws SAXException, IOException {

		try {
			validate("invalid_extractor_ordering_v1.xml", SCHEMA_V1);
			fail("Should have failed on bad ordering");
		} catch (SAXParseException e) {
			assertEquals(7, e.getLineNumber());
		}
	}
}
