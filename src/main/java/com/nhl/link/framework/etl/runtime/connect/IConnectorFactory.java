package com.nhl.link.framework.etl.runtime.connect;

import com.nhl.link.framework.etl.connect.Connector;

public interface IConnectorFactory {

	Connector createConnector(String id);
}
