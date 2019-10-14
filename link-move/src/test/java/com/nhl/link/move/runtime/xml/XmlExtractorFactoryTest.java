package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.connect.StreamConnector;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.when;

public class XmlExtractorFactoryTest extends BaseExtractorFactoryTest<StreamConnector, XmlExtractorFactory> {
	private static final String XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<test></test>";

	@Override
	protected XmlExtractorFactory createExtractorFactory() {
		return new XmlExtractorFactory();
	}

	@Override
	protected Class<StreamConnector> getConnectorType() {
		return StreamConnector.class;
	}

	@Override
	public void setUpExtractorFactory() {
		super.setUpExtractorFactory();
		try {
			when(getConnectorMock().getInputStream(anyMap()))
					.thenReturn(new ByteArrayInputStream(XML.getBytes()));
		} catch (IOException e) {
			fail("Unexpected IO exception");
		}
	}

	@Override
	public void setUpExtractorModel() {
		super.setUpExtractorModel();
		getModel().addProperty(XmlExtractorFactory.XPATH_EXPRESSION_PROPERTY, "/test");
	}

	@Test
	public void testGetConnectorType() {
		assertEquals(getConnectorType(), getExtractorFactory().getConnectorType());
	}

	@Test
	public void testCreateExtractor() {
		extractorFactory.createExtractor(connectorMock, getModel());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreateExtractorWithEmptyXPathExpression() {
		getModel().clearProperties();
		getExtractorFactory().createExtractor(getConnectorMock(), getModel());
	}
}