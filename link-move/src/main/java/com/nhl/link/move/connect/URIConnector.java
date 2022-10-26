package com.nhl.link.move.connect;

import com.nhl.link.move.LmRuntimeException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Map;

/**
 * @since 1.4
 */
public class URIConnector implements StreamConnector {

    private final URI uri;

    public URIConnector(URI uri) {
        this.uri = uri;
    }

    @Override
    public InputStream getInputStream(Map<String, ?> parameters) throws IOException {
        URL url;

        try {
            url = uri.toURL();
        } catch (IllegalArgumentException e) {
            throw new LmRuntimeException("Error converting URI to URL: " + uri, e);
        }

        return url.openStream();
    }
}
