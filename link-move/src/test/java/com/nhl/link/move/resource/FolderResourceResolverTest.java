package com.nhl.link.move.resource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.*;

public class FolderResourceResolverTest {

    private FolderResourceResolver resolver;
    private File rootDir;

    @BeforeEach
    public void before() throws URISyntaxException {
        URL extractorResource = getClass().getResource("dummy.xml");
        assertNotNull(extractorResource);

        this.rootDir = new File(extractorResource.toURI()).getParentFile();
        assertTrue(rootDir.isDirectory());

        this.resolver = new FolderResourceResolver(rootDir);
    }

    @Test
    public void testReader() throws IOException {

        try (Reader r = resolver.reader("dummy.xml")) {

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
