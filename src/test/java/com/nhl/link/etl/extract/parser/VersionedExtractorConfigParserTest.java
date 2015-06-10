package com.nhl.link.etl.extract.parser;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;

import com.nhl.link.etl.extract.ExtractorConfig;

public class VersionedExtractorConfigParserTest extends BaseParserTest {

	private VersionedExtractorConfigParser parser;

	private DOMExtractorConfigParser mockNSDefault;
	private DOMExtractorConfigParser mockNS1;
	private DOMExtractorConfigParser mockNS2;

	@Before
	public void before() {

		mockNSDefault = mock(DOMExtractorConfigParser.class);
		mockNS1 = mock(DOMExtractorConfigParser.class);
		mockNS2 = mock(DOMExtractorConfigParser.class);

		Map<String, DOMExtractorConfigParser> parsersByNS = new HashMap<>();
		parsersByNS.put(VersionedExtractorConfigParser.NO_NS_PARSER_NS, mockNSDefault);
		parsersByNS.put("ns1", mockNS1);
		parsersByNS.put("ns2", mockNS2);

		this.parser = new VersionedExtractorConfigParser(parsersByNS);
	}

	@Test
	public void testParse_NoXSD() {

		Element rootElement = getXmlRoot("extractor_v1_no_xsd.xml");
		ExtractorConfig config = new ExtractorConfig("aname");

		parser.parse(rootElement, config);

		verify(mockNSDefault).parse(rootElement, config);
		verifyZeroInteractions(mockNS1, mockNS2);
	}
	
	@Test
	public void testParse_NS1_XSD() {

		Element rootElement = getXmlRoot("extractor_dummy_ns1_xsd.xml");
		ExtractorConfig config = new ExtractorConfig("aname");

		parser.parse(rootElement, config);

		verify(mockNS1).parse(rootElement, config);
		verifyZeroInteractions(mockNSDefault, mockNS2);
	}
	
	@Test
	public void testParse_NS2_XSD() {

		Element rootElement = getXmlRoot("extractor_dummy_ns2_xsd.xml");
		ExtractorConfig config = new ExtractorConfig("aname");

		parser.parse(rootElement, config);

		verify(mockNS2).parse(rootElement, config);
		verifyZeroInteractions(mockNSDefault, mockNS1);
	}
}
