package com.nhl.link.framework.etl.runtime.connect;

import com.nhl.link.framework.etl.connect.Connector;

public interface IConnectorService {

	<T extends Connector> T getConnector(Class<T> type, String id);
}
