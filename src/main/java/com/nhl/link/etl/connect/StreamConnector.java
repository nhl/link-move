package com.nhl.link.etl.connect;

import java.io.IOException;
import java.io.InputStream;

public interface StreamConnector extends Connector {
	public InputStream getInputStream() throws IOException;
}
