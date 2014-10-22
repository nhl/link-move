package com.nhl.link.etl.runtime.jdbc;

import org.apache.cayenne.ObjectContext;

import com.nhl.link.etl.connect.Connector;

/**
 * A {@link Connector} that provides access to a JDBC data source via Cayenne
 * API.
 */
public interface JdbcConnector extends Connector {

	ObjectContext sharedContext();
}
