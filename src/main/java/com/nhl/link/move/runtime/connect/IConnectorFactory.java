package com.nhl.link.move.runtime.connect;

import com.nhl.link.move.EtlRuntimeException;
import com.nhl.link.move.connect.Connector;

/**
 * A factory to create connectors of a related type.
 * 
 * @since 1.1
 *
 * @param <C>
 *            a broad type of connector handled by this factory. It shouldn't
 *            have to be the exact class of connector, but perhaps a
 *            subinterface like JdbcConnector, LdapConnector, etc.
 */
public interface IConnectorFactory<C extends Connector> {

	/**
	 * Creates a connector for a given symbolic ID. If a connector can not be
	 * created, or an ID is not recognized, {@link EtlRuntimeException} is
	 * thrown.
	 */
	C createConnector(String id);
}
