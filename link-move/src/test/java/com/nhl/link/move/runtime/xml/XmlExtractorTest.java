package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.StreamConnector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

public class XmlExtractorTest {

	private XmlExtractor xmlExtractor;
	private InputStream inputStreamMock;
	private XPathExpression xPathExpressionMock;

	@BeforeEach
	public void setUpXmlExtractor() throws IOException {
		inputStreamMock = mock(InputStream.class);
		StreamConnector streamConnectorMock = mock(StreamConnector.class);
		when(streamConnectorMock.getInputStream(anyMap())).thenReturn(inputStreamMock);
		xPathExpressionMock = mock(XPathExpression.class);
		xmlExtractor = new XmlExtractor(streamConnectorMock, new XmlRowAttribute[0], xPathExpressionMock);
	}

	@Test
	public void testGetReader() throws Exception {
		RowReader reader = xmlExtractor.getReader(new HashMap<>());
		verify(xPathExpressionMock).evaluate(argThat(new ArgumentMatcher<InputSource>() {
			@Override
			public boolean matches(Object argument) {
				return ((InputSource) argument).getByteStream() == inputStreamMock;
			}
		}), eq(XPathConstants.NODESET));
		assertNotNull(reader);
	}
}