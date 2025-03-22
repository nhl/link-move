package com.nhl.link.move.runtime.connect;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.Connector;

import java.util.List;
import java.util.Optional;

/**
 * @since 3.0.0
 */
public class ChainConnectorFactory<C extends Connector> implements IConnectorFactory<C> {

    private final Class<C> connectorType;
    private final List<IConnectorFactory<C>> factories;

    public ChainConnectorFactory(Class<C> connectorType, List<IConnectorFactory<C>> factories) {
        this.connectorType = connectorType;
        this.factories = factories;
    }

    @Override
    public Class<C> getConnectorType() {
        return connectorType;
    }

    @Override
    public Optional<? extends C> createConnector(String id) throws LmRuntimeException {
        for (IConnectorFactory<? extends C> f : factories) {
            Optional<? extends C> connector = f.createConnector(id);
            if (connector.isPresent()) {
                return connector;
            }
        }
        return Optional.empty();
    }
}