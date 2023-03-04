package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.runtime.connect.IConnectorFactory;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Optional;

/**
 * A factory for {@link JdbcConnector} variety of Connectors based on a map of
 * DataSources. Keys in the map correspond to connector IDs.
 *
 * @since 1.1
 */
public class DataSourceConnectorFactory implements IConnectorFactory<JdbcConnector> {

    private final Map<String, DataSource> dataSources;

    public DataSourceConnectorFactory(Map<String, DataSource> dataSources) {
        this.dataSources = dataSources;
    }

    @Override
    public Class<JdbcConnector> getConnectorType() {
        return JdbcConnector.class;
    }

    @Override
    public Optional<JdbcConnector> createConnector(String id) {
        return connectorDataSource(id).map(ds -> new DataSourceConnector(id, ds));
    }

    Optional<DataSource> connectorDataSource(String id) {
        DataSource ds = dataSources.get(id);
        return Optional.ofNullable(ds);
    }
}
