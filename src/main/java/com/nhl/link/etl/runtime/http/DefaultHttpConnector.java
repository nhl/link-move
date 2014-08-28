package com.nhl.link.etl.runtime.http;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class DefaultHttpConnector implements HttpConnector {

	private final URI uri;
	private HttpClient httpClient;

	public DefaultHttpConnector(URI uri) {
		this.uri = uri;
		httpClient = new DefaultHttpClient();
	}

	@Override
	public InputStream getInputStream() throws IOException {
		HttpGet get = new HttpGet(uri);
		HttpResponse response = httpClient.execute(get);
		return response.getEntity().getContent();
	}

	@Override
	public void shutdown() {

	}
}
