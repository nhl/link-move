package com.nhl.link.move.runtime.extractor.model;

import com.nhl.link.move.extractor.model.ExtractorModelContainer;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Deprecated
public class FileExtractorModelLoaderTest {

	private FileExtractorModelLoader loader;
	private File rootDir;

	@Before
	public void before() throws URISyntaxException {
		URL extractorResource = getClass().getResource("dummy.xml");
		assertNotNull(extractorResource);

		this.rootDir = new File(extractorResource.toURI()).getParentFile();
		assertTrue(rootDir.isDirectory());

		this.loader = new FileExtractorModelLoader(rootDir);
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

	@Test
	public void testNeedsReload() {

		ExtractorModelContainer mockContainer = mock(ExtractorModelContainer.class);
		when(mockContainer.getLoadedOn()).thenReturn(System.currentTimeMillis() + 1);
		when(mockContainer.getLocation()).thenReturn("dummy.xml");

		assertFalse(loader.needsReload(mockContainer));

		File file = new File(rootDir, "dummy.xml");
		when(mockContainer.getLoadedOn()).thenReturn(file.lastModified() - 1);
		assertTrue(loader.needsReload(mockContainer));
	}
}
