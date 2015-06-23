package com.nhl.link.move.extractor.parser;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;

import com.nhl.link.move.extractor.parser.DOMExtractorModelParser;
import com.nhl.link.move.extractor.parser.ExtractorModelParser_v1;
import com.nhl.link.move.extractor.parser.VersionedExtractorModelParser;

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
		parsersByNS.put(ExtractorModelParser_v1.NS, mockNSDefault);
		parsersByNS.put("ns1", mockNS1);
		parsersByNS.put("ns2", mockNS2);

		this.parser = new VersionedExtractorModelParser(parsersByNS, ExtractorModelParser_v1.NS);
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
