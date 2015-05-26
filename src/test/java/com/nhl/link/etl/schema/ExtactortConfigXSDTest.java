package com.nhl.link.etl.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class ExtactortConfigXSDTest {

	private Validator validator;

	@Before
	public void before() throws SAXException {

		SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

		URL schemaUrl = getClass().getResource("extractor_config.xsd");
		assertNotNull(schemaUrl);

		Schema schema = schemaFactory.newSchema(schemaUrl);
		this.validator = schema.newValidator();
	}

	@Test
	public void testValid() throws SAXException, IOException {
		URL xmlUrl = getClass().getResource("valid_extractor.xml");
		assertNotNull(xmlUrl);

		try (InputStream in = xmlUrl.openStream()) {
			validator.validate(new StreamSource(in));
		}
	}

	@Test
	public void testValid_AttributeNoSource() throws SAXException, IOException {
		URL xmlUrl = getClass().getResource("valid_extractor_attribute_no_source.xml");
		assertNotNull(xmlUrl);

		try (InputStream in = xmlUrl.openStream()) {
			validator.validate(new StreamSource(in));
		}
	}

	@Test
	public void testValid_AttributeNoTarget() throws SAXException, IOException {
		URL xmlUrl = getClass().getResource("valid_extractor_attribute_no_target.xml");
		assertNotNull(xmlUrl);

		try (InputStream in = xmlUrl.openStream()) {
			validator.validate(new StreamSource(in));
		}
	}

	@Test
	public void testValid_AttributeNoSourceOrTarget() throws SAXException, IOException {
		URL xmlUrl = getClass().getResource("valid_extractor_attribute_no_source_or_target.xml");
		assertNotNull(xmlUrl);

		try (InputStream in = xmlUrl.openStream()) {
			validator.validate(new StreamSource(in));
		}
	}

	@Test
	public void testIncorrectOrdering() throws SAXException, IOException {
		URL xmlUrl = getClass().getResource("invalid_extractor_ordering.xml");
		assertNotNull(xmlUrl);

		try (InputStream in = xmlUrl.openStream()) {
			validator.validate(new StreamSource(in));
			fail("Should have failed on bad ordering");
		} catch (SAXParseException e) {
			assertEquals(7, e.getLineNumber());
		}
	}
}
