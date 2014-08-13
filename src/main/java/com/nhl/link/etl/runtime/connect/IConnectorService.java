package com.nhl.link.etl.runtime.connect;

import com.nhl.link.etl.connect.Connector;

public interface IConnectorService {

	<T extends Connector> T getConnector(Class<T> type, String id);
}
