package com.nhl.link.etl.runtime.http;

import com.nhl.link.etl.connect.Connector;

import java.io.IOException;
import java.io.InputStream;

public interface HttpConnector extends Connector {
	InputStream getInputStream() throws IOException;
}
