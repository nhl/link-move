package com.nhl.link.etl.runtime.connect;

import com.nhl.link.etl.connect.Connector;

public interface IConnectorFactory {

	Connector createConnector(String id);
}
