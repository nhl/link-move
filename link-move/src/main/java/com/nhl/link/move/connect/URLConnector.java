package com.nhl.link.move.connect;

import com.nhl.link.move.LmRuntimeException;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

/**
 * @since 3.0
 */
public class URLConnector implements StreamConnector {

    private final URL url;

    public static URLConnector of(String url) {
        try {
            return new URLConnector(new URL(url));
        } catch (MalformedURLException e) {
            throw new LmRuntimeException("Bad URL: " + url, e);
        }
    }

    public static URLConnector of(URL url) {
        return new URLConnector(url);
    }

    protected URLConnector(URL url) {
        this.url = url;
    }

    @Override
    public InputStream getInputStream(Map<String, ?> parameters) throws IOException {
        return url.openStream();
    }
}
