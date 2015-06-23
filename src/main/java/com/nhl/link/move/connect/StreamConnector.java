package com.nhl.link.move.connect;

import java.io.IOException;
import java.io.InputStream;

/**
 * @since 1.4
 */
public interface StreamConnector extends Connector {

	InputStream getInputStream() throws IOException;

}
