package com.nhl.link.move.resource;

import com.nhl.link.move.LmRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * @since 2.4
 */
public class URLResourceResolver implements ResourceResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(URLResourceResolver.class);

    private final URL baseUrl;

    public URLResourceResolver(URL baseUrl) {
        this.baseUrl = baseUrl;
        LOGGER.debug("Resources will be located relative to URL {}", baseUrl);
    }

    @Override
    public Reader reader(String location) {

        URL modelUrl;
        try {
            modelUrl = new URL(baseUrl, location);
        } catch (MalformedURLException e) {
            throw new LmRuntimeException("Error building resource URL", e);
        }

        LOGGER.debug("Will read resource at URL {}", modelUrl);

        try {
            return new InputStreamReader(modelUrl.openStream(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new LmRuntimeException("Error reading resource at URL " + modelUrl, e);
        }
    }
}
