package com.nhl.link.move.resource;

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

public class FileResourceResolverTest {

    private FileResourceResolver resolver;
    private File rootDir;

    @Before
    public void before() throws URISyntaxException {
        URL extractorResource = getClass().getResource("dummy.xml");
        assertNotNull(extractorResource);

        this.rootDir = new File(extractorResource.toURI()).getParentFile();
        assertTrue(rootDir.isDirectory());

        this.resolver = new FileResourceResolver(rootDir);
    }

    @Test
    public void testGetXmlSource() throws IOException {

        try (Reader r = resolver.reader("dummy.xml")) {

            assertNotNull(r);

            char[] buffer = new char[100];
            int read = r.read(buffer, 0, buffer.length);
            assertEquals("<dummy/>", new String(buffer, 0, read));
        }
    }

    @Test
    public void testGetXmlSource_NoExtension() throws IOException {

        try (Reader r = resolver.reader("dummy")) {

            assertNotNull(r);

            char[] buffer = new char[100];
            int read = r.read(buffer, 0, buffer.length);
            assertEquals("<dummy/>", new String(buffer, 0, read));
        }
    }

    @Test
    public void testNeedsReload() {
        assertFalse(resolver.needsReload("dummy.xml", System.currentTimeMillis() + 1));
        File file = new File(rootDir, "dummy.xml");
        assertTrue(resolver.needsReload("dummy.xml", file.lastModified() - 1));
    }
}
