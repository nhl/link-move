package com.nhl.link.move.runtime.connect;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.Connector;

import java.util.Objects;
import java.util.Optional;

/**
 * @since 3.0
 */
public class ConnectorInstanceFactory<C extends Connector> implements IConnectorFactory<C> {

    private final Class<C> connectorType;
    private final String id;
    private final C connector;

    public ConnectorInstanceFactory(Class<C> connectorType, String id, C connector) {
        this.connectorType = Objects.requireNonNull(connectorType);
        this.id = Objects.requireNonNull(id);
        this.connector = Objects.requireNonNull(connector);
    }

    @Override
    public Class<C> getConnectorType() {
        return connectorType;
    }

    @Override
    public Optional<? extends C> createConnector(String id) throws LmRuntimeException {
        return this.id.equals(id) ? Optional.of(connector) : Optional.empty();
    }
}
