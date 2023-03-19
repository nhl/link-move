package com.nhl.link.move.xsd.v2;

import com.nhl.link.move.xsd.BaseSchemaTest;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class ExtactortConfigXSDTest extends BaseSchemaTest {

	private static final String SCHEMA = "com/nhl/link/move/xsd/extractor_config_2.xsd";

	@Test
	public void testValid() throws SAXException, IOException {
		validate("valid_extractor.xml", SCHEMA);
	}

	@Test
	public void testValid_AttributeNoSource() throws SAXException, IOException {
		validate("valid_extractor_attribute_no_source.xml", SCHEMA);
	}

	@Test
	public void testValid_AttributeNoTarget() throws SAXException, IOException {
		validate("valid_extractor_attribute_no_target.xml", SCHEMA);
	}

	@Test
	public void testValid_AttributeNoSourceOrTarget() throws SAXException, IOException {
		validate("valid_extractor_attribute_no_source_or_target.xml", SCHEMA);
	}

	@Test
	public void testIncorrectOrdering() throws SAXException, IOException {

		try {
			validate("invalid_extractor_ordering.xml", SCHEMA);
			fail("Should have failed on bad ordering");
		} catch (SAXParseException e) {
			assertEquals(8, e.getLineNumber());
		}
	}
}

