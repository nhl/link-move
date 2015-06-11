package com.nhl.link.etl.runtime.extractor.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.Reader;

import org.junit.Test;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.runtime.extractor.model.ClasspathExtractorModelLoader;

public class ClasspathExtractorConfigLoaderTest {

	@Test
	public void testGetXmlSource() throws IOException {

		ClasspathExtractorModelLoader loader = new ClasspathExtractorModelLoader();

		try (Reader r = loader
				.getXmlSource("com/nhl/link/etl/runtime/extractor/loader/ClasspathExtractorConfigLoaderTest")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummy/>", new String(buffer, 0, read));
		}

	}

	@Test
	public void testGetXmlSource_WithExtension() throws IOException {

		ClasspathExtractorModelLoader loader = new ClasspathExtractorModelLoader();

		try (Reader r = loader
				.getXmlSource("com/nhl/link/etl/runtime/extractor/loader/ClasspathExtractorConfigLoaderTest.xml")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummy/>", new String(buffer, 0, read));
		}

	}

	@Test(expected = EtlRuntimeException.class)
	public void testGetXmlSource_Invalid() throws IOException {

		ClasspathExtractorModelLoader loader = new ClasspathExtractorModelLoader();
		loader.getXmlSource("no-such-resource");
	}

	@Test
	public void testGetXmlSource_Path() throws IOException {

		ClasspathExtractorModelLoader loader = new ClasspathExtractorModelLoader();

		try (Reader r = loader
				.getXmlSource("com/nhl/link/etl/runtime/extractor/loader/ClasspathExtractorConfigLoaderTest_path")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummypath/>", new String(buffer, 0, read));
		}

	}
}
