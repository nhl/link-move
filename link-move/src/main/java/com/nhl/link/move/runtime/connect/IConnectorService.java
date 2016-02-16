package com.nhl.link.move.runtime.connect;

import com.nhl.link.move.connect.Connector;

public interface IConnectorService {

	<T extends Connector> T getConnector(Class<T> type, String id);
}
