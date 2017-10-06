package com.nhl.link.move.resource;

import com.nhl.link.move.LmRuntimeException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.Reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ClasspathResourceResolverTest {

	private ClasspathResourceResolver loader;

	@Before
	public void before() {
		loader = new ClasspathResourceResolver();
	}

	@Test
	public void testGetXmlSource() throws IOException {

		try (Reader r = loader.reader("com/nhl/link/move/resource/dummy")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummy/>", new String(buffer, 0, read));
		}
	}

	@Test
	public void testGetXmlSource_WithExtension() throws IOException {

		try (Reader r = loader.reader("com/nhl/link/move/resource/dummy.xml")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummy/>", new String(buffer, 0, read));
		}

	}

	@Test(expected = LmRuntimeException.class)
	public void testGetXmlSource_Invalid() throws IOException {
		loader.reader("no-such-resource");
	}
}
