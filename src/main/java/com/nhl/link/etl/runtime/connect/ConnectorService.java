package com.nhl.link.etl.runtime.connect;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cayenne.di.Inject;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.connect.Connector;
import com.nhl.link.etl.runtime.EtlRuntimeBuilder;

public class ConnectorService implements IConnectorService {

	private ConcurrentMap<String, Connector> connectors;
	private Map<String, IConnectorFactory> factories;

	public ConnectorService(
			@Inject(EtlRuntimeBuilder.CONNECTOR_FACTORIES_MAP) Map<String, IConnectorFactory> factories,
			@Inject(EtlRuntimeBuilder.CONNECTORS_MAP) Map<String, Connector> connectors) {
		this.factories = factories;
		this.connectors = new ConcurrentHashMap<>(connectors);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Connector> T getConnector(Class<T> type, String id) {
		Connector connector = connectors.get(id);

		if (id == null) {
			throw new EtlRuntimeException("Null connector id");
		}

		if (connector == null) {

			IConnectorFactory factory = factories.get(type.getName());
			if (factory == null) {
				throw new IllegalStateException("No factory mapped for Connector type of '" + type.getName() + "'");
			}

			Connector newConnector = factory.createConnector(id);
			Connector oldConnector = connectors.putIfAbsent(id, newConnector);

			if (oldConnector != null) {
				connector = oldConnector;
				newConnector.shutdown();
			} else {
				connector = newConnector;
			}
		}

		if (!type.isAssignableFrom(connector.getClass())) {
			throw new EtlRuntimeException("Connector for id '" + id + "' is not a " + type.getName()
					+ ". The actual type is " + connector.getClass().getName());
		}

		return (T) connector;
	}
}
