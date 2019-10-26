package com.nhl.link.move.connect;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * @since 1.4
 */
@FunctionalInterface
public interface StreamConnector extends Connector {

	/**
	 * @since 2.8
	 */
	InputStream getInputStream(Map<String, ?> parameters) throws IOException;
}
