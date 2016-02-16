package com.nhl.link.move.runtime.jdbc;

import java.util.Map;

import javax.sql.DataSource;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.runtime.connect.IConnectorFactory;

/**
 * A factory for {@link JdbcConnector} variety of Connectors based on a map of
 * DataSources. Keys in the map correspond to connector IDs.
 * 
 * @since 1.1
 */
public class DataSourceConnectorFactory implements IConnectorFactory<JdbcConnector> {

	private Map<String, DataSource> dataSources;

	public DataSourceConnectorFactory(Map<String, DataSource> dataSources) {
		this.dataSources = dataSources;
	}

	@Override
	public JdbcConnector createConnector(String id) {
		return new DataSourceConnector(connectorDataSource(id));
	}

	DataSource connectorDataSource(String id) {

		DataSource ds = dataSources.get(id);

		if (ds == null) {
			throw new LmRuntimeException("Unknown connector ID: " + id + "; available IDs: " + dataSources.keySet());
		}

		return ds;
	}

}
