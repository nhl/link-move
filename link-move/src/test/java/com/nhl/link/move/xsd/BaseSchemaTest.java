package com.nhl.link.move.xsd;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.xml.sax.SAXException;

public class BaseSchemaTest {

	protected Validator createValidator(String schemaName) throws SAXException {

		SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

		URL schemaUrl = getClass().getResource(schemaName);
		assertNotNull(schemaUrl);

		Schema schema = schemaFactory.newSchema(schemaUrl);
		return schema.newValidator();
	}

	protected void validate(String resourceName, String schemaName) throws SAXException, IOException {
		URL xmlUrl = getClass().getResource(resourceName);
		assertNotNull(xmlUrl);

		try (InputStream in = xmlUrl.openStream()) {
			createValidator(schemaName).validate(new StreamSource(in));
		}
	}
}
