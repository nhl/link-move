package com.nhl.link.move.resource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class URLResourceResolverTest {

    private URLResourceResolver loader;

	@BeforeEach
	public void before() throws URISyntaxException, MalformedURLException {

		URL extractorResource = getClass().getResource("dummy.xml");
		assertNotNull(extractorResource);

		URL rootUrl = new File(extractorResource.toURI()).getParentFile().toURI().toURL();
		this.loader = new URLResourceResolver(rootUrl);
	}

	@Test
	public void testGetXmlSource() throws IOException {

		try (Reader r = loader.reader("dummy.xml")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummy/>", new String(buffer, 0, read));
		}
	}

	@Test
	public void testGetXmlSource_NoExtension() throws IOException {

		try (Reader r = loader.reader("dummy.xml")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummy/>", new String(buffer, 0, read));
		}
	}
}
