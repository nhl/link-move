package com.nhl.link.move.resource;

import com.nhl.link.move.LmRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * A {@link ResourceResolver} that reads files from CLASSPATH.
 *
 * @since 2.4
 */
public class ClasspathResourceResolver implements ResourceResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClasspathResourceResolver.class);

    public ClasspathResourceResolver() {
        LOGGER.debug("Resources will be located relative to classpath");
    }

    @Override
    public Reader reader(String name) {

        URL resource = ClasspathResourceResolver.class.getClassLoader().getResource(name);
        if (resource == null) {
            throw new LmRuntimeException("Resource not found in classpath: " + name);
        }

        LOGGER.debug("Will read resource at classpath URL {}", resource);

        try {
            return new InputStreamReader(resource.openStream(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new LmRuntimeException("Error reading resource at classpath URL " + resource, e);
        }
    }
}
