package com.nhl.link.move.runtime.extractor.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.Reader;

import org.junit.Before;
import org.junit.Test;

import com.nhl.link.move.EtlRuntimeException;
import com.nhl.link.move.runtime.extractor.model.ClasspathExtractorModelLoader;

public class ClasspathExtractorConfigLoaderTest {

	private ClasspathExtractorModelLoader loader;

	@Before
	public void before() {
		loader = new ClasspathExtractorModelLoader();
	}

	@Test
	public void testGetXmlSource() throws IOException {

		try (Reader r = loader.getXmlSource("com/nhl/link/move/runtime/extractor/model/dummy")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummy/>", new String(buffer, 0, read));
		}
	}

	@Test
	public void testGetXmlSource_WithExtension() throws IOException {

		try (Reader r = loader.getXmlSource("com/nhl/link/move/runtime/extractor/model/dummy.xml")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummy/>", new String(buffer, 0, read));
		}

	}

	@Test(expected = EtlRuntimeException.class)
	public void testGetXmlSource_Invalid() throws IOException {
		loader.getXmlSource("no-such-resource");
	}
}
