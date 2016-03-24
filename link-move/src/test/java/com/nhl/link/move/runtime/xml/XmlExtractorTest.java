package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.StreamConnector;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class XmlExtractorTest {

	private XmlExtractor xmlExtractor;

	private StreamConnector streamConnectorMock;

	private InputStream inputStreamMock;

	private XPathExpression xPathExpressionMock;

	@Before
	public void setUpXmlExtractor() throws IOException {
		inputStreamMock = mock(InputStream.class);
		streamConnectorMock = mock(StreamConnector.class);
		when(streamConnectorMock.getInputStream()).thenReturn(inputStreamMock);
		xPathExpressionMock = mock(XPathExpression.class);
		xmlExtractor = new XmlExtractor(streamConnectorMock, new XmlRowAttribute[0], xPathExpressionMock);
	}

	@Test
	public void testGetReader() throws Exception {
		RowReader reader = xmlExtractor.getReader(new HashMap<String, Object>());
		verify(xPathExpressionMock).evaluate(argThat(new ArgumentMatcher<InputSource>() {
			@Override
			public boolean matches(Object argument) {
				return ((InputSource) argument).getByteStream() == inputStreamMock;
			}
		}), eq(XPathConstants.NODESET));
		assertNotNull(reader);
	}
}