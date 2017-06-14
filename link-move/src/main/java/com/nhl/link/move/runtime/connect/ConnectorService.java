package com.nhl.link.move.runtime.connect;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.Connector;
import org.apache.cayenne.di.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConnectorService implements IConnectorService {

	private ConcurrentMap<String, Connector> connectors;
	private Map<String, IConnectorFactory> factories;

	public ConnectorService(
			@Inject Map<String, IConnectorFactory> factories,
			@Inject Map<String, Connector> connectors) {
		this.factories = factories;
		this.connectors = new ConcurrentHashMap<>(connectors);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Connector> T getConnector(Class<T> type, String id) {

		if (id == null) {
			throw new LmRuntimeException("Null connector id");
		}

		Connector connector = connectors.get(id);
		if (connector == null) {

			IConnectorFactory<?> factory = factories.get(type.getName());
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
			throw new LmRuntimeException("Connector for id '" + id + "' is not a " + type.getName()
					+ ". The actual type is " + connector.getClass().getName());
		}

		return (T) connector;
	}
}
