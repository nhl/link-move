package com.nhl.link.move.xsd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class ExtactortConfigXSD_v2Test extends BaseSchemaTest {

	private static final String SCHEMA_V2 = "extractor_config_2.xsd";

	@Test
	public void testValid() throws SAXException, IOException {
		validate("valid_extractor.xml", SCHEMA_V2);
	}

	@Test
	public void testValid_AttributeNoSource() throws SAXException, IOException {
		validate("valid_extractor_attribute_no_source.xml", SCHEMA_V2);
	}

	@Test
	public void testValid_AttributeNoTarget() throws SAXException, IOException {
		validate("valid_extractor_attribute_no_target.xml", SCHEMA_V2);
	}

	@Test
	public void testValid_AttributeNoSourceOrTarget() throws SAXException, IOException {
		validate("valid_extractor_attribute_no_source_or_target.xml", SCHEMA_V2);
	}

	@Test
	public void testIncorrectOrdering() throws SAXException, IOException {

		try {
			validate("invalid_extractor_ordering.xml", SCHEMA_V2);
			fail("Should have failed on bad ordering");
		} catch (SAXParseException e) {
			assertEquals(7, e.getLineNumber());
		}
	}
}

