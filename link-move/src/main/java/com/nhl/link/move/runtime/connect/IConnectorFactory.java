package com.nhl.link.move.runtime.connect;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.Connector;

import java.util.Optional;

/**
 * A factory to create connectors of a related type.
 *
 * @param <C> a broad type of connector handled by this factory. It shouldn't
 *            have to be the exact class of connector, but perhaps a
 *            subinterface like JdbcConnector, LdapConnector, etc.
 * @since 1.1
 */
public interface IConnectorFactory<C extends Connector> {

    /**
     * @since 3.0
     */
    Class<C> getConnectorType();

    /**
     * Creates a connector for a given symbolic ID. If an ID is not recognized, an empty Optional is returned. If a
     * connector can not be created, {@link LmRuntimeException} is thrown.
     *
     * @since 3.0
     */
    Optional<? extends C> createConnector(String id) throws LmRuntimeException;
}
