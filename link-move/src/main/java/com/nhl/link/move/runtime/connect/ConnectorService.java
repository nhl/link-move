package com.nhl.link.move.runtime.connect;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.Connector;

import java.util.Map;

public class ConnectorService implements IConnectorService {

    private final Map<Class<? extends Connector>, IConnectorFactory<?>> factories;

    public ConnectorService(Map<Class<? extends Connector>, IConnectorFactory<?>> factories) {
        this.factories = factories;
    }

    @Override
    public <T extends Connector> T getConnector(Class<T> type, String id) {

        if (id == null) {
            throw new LmRuntimeException("Null connector id");
        }

        IConnectorFactory<T> factory = (IConnectorFactory<T> ) factories.get(type);
        if (factory == null) {
            throw new IllegalStateException("No factory exists for Connector type of '" + type.getName() + "'");
        }

        return factory.createConnector(id)
                .orElseThrow(() -> new LmRuntimeException("Can't create connector for type '" + type.getName() + "' and id '" + id + "'"));
    }
}
