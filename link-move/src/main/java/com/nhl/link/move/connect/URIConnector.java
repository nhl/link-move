package com.nhl.link.move.connect;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
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
		return uri.toURL().openStream();
	}
}
