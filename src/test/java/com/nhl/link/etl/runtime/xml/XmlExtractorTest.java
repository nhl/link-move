package com.nhl.link.etl.runtime.xml;

import com.nhl.link.etl.RowAttribute;
import com.nhl.link.etl.RowReader;
import com.nhl.link.etl.extract.ExtractorParameters;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class XmlExtractorTest {

	private XmlExtractor xmlExtractor;

	private InputSource inputSource;

	private XPathExpression xPathExpressionMock;

	@Before
	public void setUpXmlExtractor() {
		inputSource = new InputSource();
		xPathExpressionMock = mock(XPathExpression.class);
		xmlExtractor = new XmlExtractor(inputSource, new RowAttribute[0], xPathExpressionMock);
	}

	@Test
	public void testGetReader() throws Exception {
		RowReader reader = xmlExtractor.getReader(new ExtractorParameters());
		verify(xPathExpressionMock).evaluate(inputSource, XPathConstants.NODESET);
		assertNotNull(reader);
	}
}