package com.nhl.link.move.resource;

import com.nhl.link.move.LmRuntimeException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

/**
 * A {@link ResourceResolver} that reads files from CLASSPATH.
 *
 * @since 2.4
 */
public class ClasspathResourceResolver implements ResourceResolver {

    @Override
    public Reader reader(String name) {

        // TODO: get rid of the XML extension convention
        if (!name.endsWith(".xml")) {
            name = name + ".xml";
        }

        URL resource = ClasspathResourceResolver.class.getClassLoader().getResource(name);
        if (resource == null) {
            throw new LmRuntimeException("Extractor config not found in classpath: " + name);
        }

        try {
            return new InputStreamReader(resource.openStream(), "UTF-8");
        } catch (IOException e) {
            throw new LmRuntimeException("Error reading classpath extractor config XML", e);
        }
    }
}
