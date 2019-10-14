package com.nhl.link.move.runtime.extractor.model;

import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorModelContainer;
import com.nhl.link.move.extractor.parser.ExtractorModelParser;
import com.nhl.link.move.extractor.parser.IExtractorModelParser;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ExtractorModelParserTest {

	private IExtractorModelParser parser;

	@Before
	public void before() {
		parser = new ExtractorModelParser();
	}

	private  ExtractorModelContainer parse(String resourceName) {
	    return parser.parse(resourceName, reader(resourceName));
    }

	private Reader reader(String resourceName) {
        InputStream in = ExtractorModelParserTest.class.getResourceAsStream(resourceName);
        assertNotNull("Resource not found: " + resourceName, in);
        return new InputStreamReader(in);
    }

	@Test
	public void testLoad_v1() {

		ExtractorModelContainer container = parse("extractor_v1.xml");
		assertNotNull(container);
		assertEquals("extractor_v1.xml", container.getLocation());
		assertEquals("atype", container.getType());
		assertTrue(container.getConnectorIds().contains("aconnector"));

		assertEquals(1, container.getExtractorNames().size());

		ExtractorModel model = container.getExtractor(ExtractorModel.DEFAULT_NAME);

		assertEquals(ExtractorModel.DEFAULT_NAME, model.getName());
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

		assertEquals(2, model.getProperties().size());
		assertEquals("AB", model.getSingletonProperty("a.b"));
		assertEquals("XY", model.getSingletonProperty("x.y"));
	}

	@Test
	public void testLoad_v2() {

        ExtractorModelContainer container = parse("extractor_v2.xml");
		assertNotNull(container);
		assertEquals("extractor_v2.xml", container.getLocation());
		assertEquals("atype", container.getType());
		assertTrue(container.getConnectorIds().contains("aconnector"));

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
		assertEquals("AB1", m1.getSingletonProperty("a.b"));
		assertEquals("XY1", m1.getSingletonProperty("x.y"));

		ExtractorModel m2 = container.getExtractor("e2");
		assertEquals("atype2", m2.getType());
		assertTrue(m2.getConnectorIds().contains("aconnector2"));
		assertNull(m2.getAttributes());

		assertEquals(2, m2.getProperties().size());
		assertEquals("AB2", m2.getSingletonProperty("a.b"));
		assertEquals("XY2", m2.getSingletonProperty("x.y"));

		ExtractorModel m3 = container.getExtractor(ExtractorModel.DEFAULT_NAME);
		assertNotNull(m3);
		assertEquals(ExtractorModel.DEFAULT_NAME, m3.getName());
	}
}
