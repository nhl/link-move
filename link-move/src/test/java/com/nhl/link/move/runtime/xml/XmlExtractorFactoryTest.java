package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.runtime.extractor.BaseExtractorFactoryTest;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.fail;
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
			when(getConnectorMock().getInputStream())
					.thenReturn(new ByteArrayInputStream(XML.getBytes()));
		} catch (IOException e) {
			fail("Unexpected IO exception");
		}
	}

	@Override
	public void setUpExtratorModel() {
		super.setUpExtratorModel();
		getModel().getProperties().put(XmlExtractorFactory.XPATH_EXPRESSION_PROPERTY, "/test");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreateExtractorWithEmptyXPathExpression() {
		getModel().getProperties().remove(XmlExtractorFactory.XPATH_EXPRESSION_PROPERTY);
		getExtractorFactory().createExtractor(getConnectorMock(), getModel());
	}
}