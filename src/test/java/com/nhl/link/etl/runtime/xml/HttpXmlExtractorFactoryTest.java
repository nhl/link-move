package com.nhl.link.etl.runtime.xml;

import com.nhl.link.etl.runtime.extract.BaseExtractorFactoryTest;
import com.nhl.link.etl.runtime.http.HttpConnector;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class HttpXmlExtractorFactoryTest extends BaseExtractorFactoryTest<HttpConnector, HttpXmlExtractorFactory> {
	private static final String XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<test></test>";

	@Override
	protected HttpXmlExtractorFactory createExtractorFactory() {
		return new HttpXmlExtractorFactory(getConnectorServiceMock());
	}

	@Override
	protected Class<HttpConnector> getConnectorType() {
		return HttpConnector.class;
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
	public void setUpExtractorConfig() {
		super.setUpExtractorConfig();
		getExtractorConfig().getProperties().put(HttpXmlExtractorFactory.XPATH_EXPRESSION_PROPERTY, "/test");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreateExtractorWithEmptyXPathExpression() {
		getExtractorConfig().getProperties().remove(HttpXmlExtractorFactory.XPATH_EXPRESSION_PROPERTY);
		getExtractorFactory().createExtractor(getConnectorMock(), getExtractorConfig());
	}
}