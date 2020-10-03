package com.nhl.link.move.extractor.parser;

import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorModelContainer;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.*;

public class ExtractorModelParser_v1Test extends BaseParserTest {

	final ExtractorModelParser_v1 parser = new ExtractorModelParser_v1();

	@Test
	public void testParse() {

		Element xmlRoot = getXmlRoot("extractor_v1.xml");

		ExtractorModelContainer container = parser.parse("alocation", xmlRoot);
		assertNotNull(container);

		assertEquals("alocation", container.getLocation());
		assertEquals("atype", container.getType());
		assertTrue(container.getConnectorIds().contains("aconnector"));

		Collection<String> extractorNames = container.getExtractorNames();
		assertEquals(1, extractorNames.size());
		assertTrue(extractorNames.contains(ExtractorModel.DEFAULT_NAME));

		ExtractorModel model = container.getExtractor(ExtractorModel.DEFAULT_NAME);

		assertEquals("atype", model.getType());
		assertTrue(model.getConnectorIds().contains("aconnector"));

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

		assertEquals("AB", model.getPropertyValue("a.b"));
		assertEquals("XY", model.getPropertyValue("x.y"));
	}
}
