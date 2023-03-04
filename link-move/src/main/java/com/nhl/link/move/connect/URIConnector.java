package com.nhl.link.move.connect;

import com.nhl.link.move.LmRuntimeException;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

/**
 * @deprecated in favor of {@link URLConnector}.
 */
@Deprecated(since = "3.0")
public class URIConnector extends URLConnector {

    public URIConnector(URI uri) {
        super(toURL(uri));
    }

    private static URL toURL(URI uri) {
        try {
            return uri.toURL();
        } catch (MalformedURLException e) {
            throw new LmRuntimeException("Error converting URI to URL: " + uri, e);
        }
    }
}
