package com.nhl.link.etl.runtime.url;

import com.nhl.link.etl.connect.StreamConnector;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class URLConnector implements StreamConnector {
	private final String urlPath;

	public URLConnector(String urlPath) {
		this.urlPath = urlPath;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		return new URL(urlPath).openStream();
	}

	@Override
	public void shutdown() {

	}
}
