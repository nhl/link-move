package com.nhl.link.etl.runtime.connect;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.connect.StreamConnector;
import com.nhl.link.etl.connect.URIConnector;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @since 1.4
 */
public class URIConnectorFactory implements IConnectorFactory<StreamConnector> {

	@Override
	public StreamConnector createConnector(String id) {

		URI uri;
		try {
			// first try to get resource from classpath
			URL url = URIConnectorFactory.class.getResource(id);
			if (url != null) {
				uri = url.toURI();
			} else {
				// probably we have absolute path to file here
				uri = new URI(id);
			}
			if (!uri.isAbsolute()) {
				throw new EtlRuntimeException(
						"Provided URI is not absolute (should be a classpath resource or absolute path to file)"
				);
			}
		} catch (URISyntaxException e) {
			throw new EtlRuntimeException("Failed to create connector for URI, because it is malformed: " + id, e);
		}

		return new URIConnector(uri);
	}

}
