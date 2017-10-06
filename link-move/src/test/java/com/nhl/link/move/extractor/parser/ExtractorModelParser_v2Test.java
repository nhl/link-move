package com.nhl.link.move.extractor.parser;

import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorModelContainer;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ExtractorModelParser_v2Test extends BaseParserTest {

	private ExtractorModelParser_v2 parser;

	@Before
	public void before() {
		this.parser = new ExtractorModelParser_v2();
	}

	@Test
	public void testParse() {

		Element xmlRoot = getXmlRoot("extractor_v2.xml");

		long t0 = System.currentTimeMillis() - 1;

		ExtractorModelContainer container = parser.parse("alocation", xmlRoot);

		long t1 = System.currentTimeMillis() + 1;

		assertNotNull(container);
		assertEquals("alocation", container.getLocation());
		assertEquals("atype", container.getType());
		assertTrue(container.getConnectorIds().contains("aconnector"));

		assertTrue(container.getLoadedOn() > t0);
		assertTrue(container.getLoadedOn() < t1);

		Collection<String> extractorNames = container.getExtractorNames();
		assertEquals(3, extractorNames.size());
		assertTrue(extractorNames.contains("e1"));
		assertTrue(extractorNames.contains("e2"));
		assertTrue(extractorNames.contains(ExtractorModel.DEFAULT_NAME));

		ExtractorModel m1 = container.getExtractor("e1");

		assertEquals("atype", m1.getType());
        assertTrue(m1.getConnectorIds().contains("aconnector"));

		assertEquals(3, m1.getAttributes().length);

		assertEquals(0, m1.getAttributes()[0].getOrdinal());
		assertEquals(String.class, m1.getAttributes()[0].type());
		assertEquals("a1", m1.getAttributes()[0].getSourceName());
		assertEquals("a_1", m1.getAttributes()[0].getTargetPath());

		assertEquals(1, m1.getAttributes()[1].getOrdinal());
		assertEquals(Integer.class, m1.getAttributes()[1].type());
		assertEquals("a2", m1.getAttributes()[1].getSourceName());
		assertEquals("a_2", m1.getAttributes()[1].getTargetPath());

		assertEquals(2, m1.getAttributes()[2].getOrdinal());
		assertEquals(Integer.class, m1.getAttributes()[2].type());
		assertEquals("a2", m1.getAttributes()[2].getSourceName());
		assertNull(m1.getAttributes()[2].getTargetPath());

		assertEquals(2, m1.getProperties().size());
		assertEquals("AB1", m1.getProperties().get("a.b"));
		assertEquals("XY1", m1.getProperties().get("x.y"));
		
		ExtractorModel m2 = container.getExtractor("e2");
		assertEquals("atype2", m2.getType());
        assertTrue(m2.getConnectorIds().contains("aconnector2"));
        assertNull(m2.getAttributes());

		assertEquals(2, m2.getProperties().size());
		assertEquals("AB2", m2.getProperties().get("a.b"));
		assertEquals("XY2", m2.getProperties().get("x.y"));	
		
		ExtractorModel m3 = container.getExtractor(ExtractorModel.DEFAULT_NAME);
		assertNotNull(m3);
		assertEquals(ExtractorModel.DEFAULT_NAME, m3.getName());
	}

}
