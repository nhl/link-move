package com.nhl.link.move.extractor.parser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class VersionedExtractorModelParserTest extends BaseParserTest {

	private VersionedExtractorModelParser parser;

	private DOMExtractorModelParser mockNSDefault;
	private DOMExtractorModelParser mockNS1;
	private DOMExtractorModelParser mockNS2;

	@BeforeEach
	public void before() {

		mockNSDefault = mock(DOMExtractorModelParser.class);
		mockNS1 = mock(DOMExtractorModelParser.class);
		mockNS2 = mock(DOMExtractorModelParser.class);

		Map<String, DOMExtractorModelParser> parsersByNS = new HashMap<>();
		parsersByNS.put(ExtractorModelParser_v1.NS, mockNSDefault);
		parsersByNS.put("ns1", mockNS1);
		parsersByNS.put("ns2", mockNS2);

		this.parser = new VersionedExtractorModelParser(parsersByNS, ExtractorModelParser_v1.NS);
	}

	@Test
	public void parse_NoXSD() {

		Element xmlRoot = getXmlRoot("extractor_v1_no_xsd.xml");

		parser.parse("alocation", xmlRoot);

		verify(mockNSDefault).parse("alocation", xmlRoot);
		verifyZeroInteractions(mockNS1, mockNS2);
	}

	@Test
	public void parse_NS1_XSD() {

		Element xmlRoot = getXmlRoot("extractor_dummy_ns1_xsd.xml");

		parser.parse("alocation", xmlRoot);

		verify(mockNS1).parse("alocation", xmlRoot);
		verifyZeroInteractions(mockNSDefault, mockNS2);
	}

	@Test
	public void parse_NS2_XSD() {

		Element xmlRoot = getXmlRoot("extractor_dummy_ns2_xsd.xml");

		parser.parse("alocation", xmlRoot);

		verify(mockNS2).parse("alocation", xmlRoot);
		verifyZeroInteractions(mockNSDefault, mockNS1);
	}
}
