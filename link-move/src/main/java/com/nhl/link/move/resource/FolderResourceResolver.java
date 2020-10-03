package com.nhl.link.move.resource;

import com.nhl.link.move.LmRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

/**
 * A {@link ResourceResolver} that loads resources from files located in a given directory.
 *
 * @since 2.4
 */
public class FolderResourceResolver implements ResourceResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(FolderResourceResolver.class);

    private final File baseDir;

    public FolderResourceResolver(File baseDir) {
        LOGGER.info("Extractor XML files will be located under '{}'", baseDir);
        this.baseDir = baseDir;
    }

    @Override
    public Reader reader(String name) {

        File file = getFile(name);

        LOGGER.info("Will extract XML from {}", file.getAbsolutePath());

        try {
            return new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new LmRuntimeException("Error reading extractor config XML from file " + file, e);
        }
    }

    @Override
    public boolean needsReload(String location, long lastLoadedOn) {
        return lastLoadedOn < getFile(location).lastModified();
    }

    protected File getFile(String name) {

        File file = new File(baseDir, name);

        if (!file.exists()) {
            throw new LmRuntimeException(file.getAbsolutePath() + " does not exist");
        }

        if (!file.isFile()) {
            throw new LmRuntimeException(file.getAbsolutePath() + " is not a file");
        }

        return file;
    }
}
