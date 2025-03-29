package com.nhl.link.move.xsd.v1;

import com.nhl.link.move.xsd.BaseSchemaTest;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class ExtractorConfigXSDTest extends BaseSchemaTest {

	private static final String SCHEMA = "com/nhl/link/move/xsd/extractor_config_1.xsd";

	@Test
	public void valid() throws SAXException, IOException {
		validate("valid_extractor.xml", SCHEMA);
	}

	@Test
	public void incorrectOrdering() throws SAXException, IOException {

		try {
			validate("invalid_extractor_ordering.xml", SCHEMA);
			fail("Should have failed on bad ordering");
		} catch (SAXParseException e) {
			assertEquals(8, e.getLineNumber());
		}
	}
}
