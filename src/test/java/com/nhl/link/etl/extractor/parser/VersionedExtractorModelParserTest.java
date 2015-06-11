package com.nhl.link.etl.extractor.parser;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;

public class VersionedExtractorModelParserTest extends BaseParserTest {

	private VersionedExtractorModelParser parser;

	private DOMExtractorModelParser mockNSDefault;
	private DOMExtractorModelParser mockNS1;
	private DOMExtractorModelParser mockNS2;

	@Before
	public void before() {

		mockNSDefault = mock(DOMExtractorModelParser.class);
		mockNS1 = mock(DOMExtractorModelParser.class);
		mockNS2 = mock(DOMExtractorModelParser.class);

		Map<String, DOMExtractorModelParser> parsersByNS = new HashMap<>();
		parsersByNS.put(VersionedExtractorModelParser.NO_NS_PARSER_NS, mockNSDefault);
		parsersByNS.put("ns1", mockNS1);
		parsersByNS.put("ns2", mockNS2);

		this.parser = new VersionedExtractorModelParser(parsersByNS);
	}

	@Test
	public void testParse_NoXSD() {

		Element xmlRoot = getXmlRoot("extractor_v1_no_xsd.xml");

		parser.parse("alocation", xmlRoot);

		verify(mockNSDefault).parse("alocation", xmlRoot);
		verifyZeroInteractions(mockNS1, mockNS2);
	}

	@Test
	public void testParse_NS1_XSD() {

		Element xmlRoot = getXmlRoot("extractor_dummy_ns1_xsd.xml");

		parser.parse("alocation", xmlRoot);

		verify(mockNS1).parse("alocation", xmlRoot);
		verifyZeroInteractions(mockNSDefault, mockNS2);
	}

	@Test
	public void testParse_NS2_XSD() {

		Element xmlRoot = getXmlRoot("extractor_dummy_ns2_xsd.xml");

		parser.parse("alocation", xmlRoot);

		verify(mockNS2).parse("alocation", xmlRoot);
		verifyZeroInteractions(mockNSDefault, mockNS1);
	}
}
