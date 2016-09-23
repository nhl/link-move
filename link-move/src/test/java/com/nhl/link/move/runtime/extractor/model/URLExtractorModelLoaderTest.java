package com.nhl.link.move.runtime.extractor.model;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class URLExtractorModelLoaderTest {

    private URLExtractorModelLoader loader;
	private URL rootUrl;

	@Before
	public void before() throws URISyntaxException, MalformedURLException {

		URL extractorResource = getClass().getResource("dummy.xml");
		assertNotNull(extractorResource);

        this.rootUrl = new File(extractorResource.toURI()).getParentFile().toURI().toURL();

		this.loader = new URLExtractorModelLoader(rootUrl);
	}

	@Test
	public void testGetXmlSource() throws IOException {

		try (Reader r = loader.getXmlSource("dummy.xml")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummy/>", new String(buffer, 0, read));
		}
	}

	@Test
	public void testGetXmlSource_NoExtension() throws IOException {

		try (Reader r = loader.getXmlSource("dummy")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummy/>", new String(buffer, 0, read));
		}
	}
}
