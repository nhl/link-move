package com.nhl.link.etl.runtime.extractor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.Reader;

import org.junit.Test;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.runtime.extractor.ClasspathExtractorConfigLoader;

public class ClasspathExtractorConfigLoaderTest {

	@Test
	public void testGetXmlSource() throws IOException {

		ClasspathExtractorConfigLoader loader = new ClasspathExtractorConfigLoader();

		try (Reader r = loader.getXmlSource("ClasspathExtractorConfigLoaderTest")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummy/>", new String(buffer, 0, read));
		}

	}

	@Test
	public void testGetXmlSource_WithExtension() throws IOException {

		ClasspathExtractorConfigLoader loader = new ClasspathExtractorConfigLoader();

		try (Reader r = loader.getXmlSource("ClasspathExtractorConfigLoaderTest.xml")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummy/>", new String(buffer, 0, read));
		}

	}

	@Test(expected = EtlRuntimeException.class)
	public void testGetXmlSource_Invalid() throws IOException {

		ClasspathExtractorConfigLoader loader = new ClasspathExtractorConfigLoader();
		loader.getXmlSource("no-such-resource");
	}

	@Test
	public void testGetXmlSource_Path() throws IOException {

		ClasspathExtractorConfigLoader loader = new ClasspathExtractorConfigLoader();

		try (Reader r = loader.getXmlSource("extractor/ClasspathExtractorConfigLoaderTest_path")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummypath/>", new String(buffer, 0, read));
		}

	}
}
