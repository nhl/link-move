package com.nhl.link.etl.runtime.extractor.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

import com.nhl.link.etl.extractor.model.ExtractorModel;
import com.nhl.link.etl.extractor.model.ExtractorModelContainer;

public class BaseExtractorModelLoaderTest {

	private BaseExtractorModelLoader loader;

	@Before
	public void before() {

		this.loader = new BaseExtractorModelLoader() {

			@Override
			protected Reader getXmlSource(String name) throws IOException {
				InputStream in = BaseExtractorModelLoaderTest.class.getResourceAsStream(name);
				assertNotNull("Resource not found: " + name, in);
				return new InputStreamReader(in);
			}

			@Override
			public boolean needsReload(ExtractorModelContainer container) {
				return true;
			}
		};
	}

	@Test
	public void testLoad_v1() {

		ExtractorModelContainer container = loader.load("extractor_v1.xml");
		assertNotNull(container);
		assertEquals("extractor_v1.xml", container.getLocation());
		assertEquals("atype", container.getType());
		assertEquals("aconnector", container.getConnectorId());

		assertEquals(1, container.getExtractorNames().size());

		ExtractorModel model = container.getExtractor(ExtractorModel.DEFAULT_NAME);

		assertEquals(ExtractorModel.DEFAULT_NAME, model.getName());
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

	@Test
	public void testLoad_v2() {

		ExtractorModelContainer container = loader.load("extractor_v2.xml");
		assertNotNull(container);
		assertEquals("extractor_v2.xml", container.getLocation());
		assertEquals("atype", container.getType());
		assertEquals("aconnector", container.getConnectorId());

		Collection<String> extractorNames = container.getExtractorNames();
		assertEquals(3, extractorNames.size());
		assertTrue(extractorNames.contains("e1"));
		assertTrue(extractorNames.contains("e2"));
		assertTrue(extractorNames.contains(ExtractorModel.DEFAULT_NAME));

		ExtractorModel m1 = container.getExtractor("e1");

		assertEquals("atype", m1.getType());
		assertEquals("aconnector", m1.getConnectorId());

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
		assertEquals("aconnector2", m2.getConnectorId());
		assertNull(m2.getAttributes());

		assertEquals(2, m2.getProperties().size());
		assertEquals("AB2", m2.getProperties().get("a.b"));
		assertEquals("XY2", m2.getProperties().get("x.y"));

		ExtractorModel m3 = container.getExtractor(ExtractorModel.DEFAULT_NAME);
		assertNotNull(m3);
		assertEquals(ExtractorModel.DEFAULT_NAME, m3.getName());
	}
}
