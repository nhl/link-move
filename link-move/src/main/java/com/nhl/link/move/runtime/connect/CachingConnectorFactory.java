package com.nhl.link.move.runtime.connect;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.Connector;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @since 3.0.0
 */
public class CachingConnectorFactory<C extends Connector> implements IConnectorFactory<C> {

    private final ConcurrentMap<String, Optional<? extends C>> connectors;
    private final IConnectorFactory<C> delegate;

    public CachingConnectorFactory(IConnectorFactory<C> delegate) {
        this.connectors = new ConcurrentHashMap<>();
        this.delegate = delegate;
    }

    @Override
    public Class<C> getConnectorType() {
        return delegate.getConnectorType();
    }

    @Override
    public Optional<? extends C> createConnector(String id) throws LmRuntimeException {
        return connectors.computeIfAbsent(id, delegate::createConnector);
    }
}
