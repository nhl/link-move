package com.nhl.link.etl.runtime.jdbc;

import javax.sql.DataSource;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.configuration.server.ServerRuntimeBuilder;
import org.apache.cayenne.di.BeforeScopeEnd;

import com.nhl.link.etl.connect.Connector;

/**
 * A {@link Connector} that provides access to a JDBC data source via Cayenne
 * API. It builds a DataSource based on JDBC connection properties and a minimal
 * map-less Cayenne stack on top of that.
 */
public class JdbcConnector implements Connector {

	private ServerRuntime runtime;
	private ObjectContext sharedContext;

	public JdbcConnector(DataSource dataSource) {
		this.runtime = new ServerRuntimeBuilder().dataSource(dataSource).build();
		this.sharedContext = runtime.newContext();
	}

	public ObjectContext sharedContext() {
		return sharedContext;
	}

	@BeforeScopeEnd
	@Override
	public void shutdown() {
		runtime.shutdown();
	}
}
