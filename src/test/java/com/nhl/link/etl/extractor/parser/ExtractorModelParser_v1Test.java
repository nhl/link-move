package com.nhl.link.etl.extractor.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;

import com.nhl.link.etl.extractor.model.ExtractorModel;
import com.nhl.link.etl.extractor.model.ExtractorModelContainer;
import com.nhl.link.etl.extractor.parser.ExtractorModelParser_v1;

public class ExtractorModelParser_v1Test extends BaseParserTest {

	private ExtractorModelParser_v1 parser;

	@Before
	public void before() {
		this.parser = new ExtractorModelParser_v1();
	}

	@Test
	public void testParse() {

		Element xmlRoot = getXmlRoot("extractor_v1.xml");

		ExtractorModelContainer container = parser.parse("alocation", xmlRoot);
		assertNotNull(container);

		assertEquals("atype", container.getType());
		assertEquals("aconnector", container.getConnectorId());

		Collection<String> extractorNames = container.getExtractorNames();
		assertEquals(1, extractorNames.size());
		assertTrue(extractorNames.contains("alocation"));
		
		ExtractorModel model = container.getExtractor("alocation");
		
		assertEquals("atype", model.getType());
		assertEquals("aconnector", model.getConnectorId());
		
		assertEquals(3, model.getAttributes().length);

		assertEquals(0, model.getAttributes()[0].getOrdinal());
		assertEquals(String.class, model.getAttributes()[0].type());
		assertEquals("a1", model.getAttributes()[0].getSourceName());
		assertEquals("a_1", model.getAttributes()[0].getTargetPath());

		assertEquals(1, model.getAttributes()[1].getOrdinal());
		assertEquals(Integer.class, model.getAttributes()[1].type());
		assertEquals("a2", model.getAttributes()[1].getSourceName());
		assertEquals("a_2", model.getAttributes()[1].getTargetPath());

		assertEquals(2, model.getAttributes()[2].getOrdinal());
		assertEquals(Integer.class, model.getAttributes()[2].type());
		assertEquals("a2", model.getAttributes()[2].getSourceName());
		assertNull(model.getAttributes()[2].getTargetPath());

		assertEquals(2, model.getProperties().size());
		assertEquals("AB", model.getProperties().get("a.b"));
		assertEquals("XY", model.getProperties().get("x.y"));
	}
}
