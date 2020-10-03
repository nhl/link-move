package com.nhl.link.move.resource;

import com.nhl.link.move.LmRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;

import static org.junit.jupiter.api.Assertions.*;

public class ClasspathResourceResolverTest {

	private ClasspathResourceResolver loader;

	@BeforeEach
	public void before() {
		loader = new ClasspathResourceResolver();
	}

	@Test
	public void testReader_WithExtension() throws IOException {

		try (Reader r = loader.reader("com/nhl/link/move/resource/dummy.xml")) {

			assertNotNull(r);

			char[] buffer = new char[100];
			int read = r.read(buffer, 0, buffer.length);
			assertEquals("<dummy/>", new String(buffer, 0, read));
		}

	}

	@Test
	public void testReader_Invalid() {
		assertThrows(LmRuntimeException.class, () -> loader.reader("no-such-resource.xml"));
	}
}
