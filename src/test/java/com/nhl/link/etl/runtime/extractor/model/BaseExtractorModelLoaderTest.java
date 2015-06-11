package com.nhl.link.etl.runtime.extractor.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

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
				return new InputStreamReader(BaseExtractorModelLoaderTest.class.getResourceAsStream(name));
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

		ExtractorModel model = container.getExtractor("extractor_v1.xml");
		
		assertEquals("extractor_v1.xml", model.getName());
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
