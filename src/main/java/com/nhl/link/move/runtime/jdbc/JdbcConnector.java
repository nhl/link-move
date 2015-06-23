package com.nhl.link.move.runtime.jdbc;

import org.apache.cayenne.ObjectContext;

import com.nhl.link.move.connect.Connector;

/**
 * A {@link Connector} that provides access to a JDBC data source via Cayenne
 * API.
 */
public interface JdbcConnector extends Connector {

	ObjectContext sharedContext();
}
