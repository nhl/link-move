package com.nhl.link.etl.connect;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * @since 1.4
 */
public class URIConnector implements StreamConnector {

	private final URI uri;

	public URIConnector(URI uri) {
		this.uri = uri;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		return uri.toURL().openStream();
	}

	@Override
	public void shutdown() {

	}
}
