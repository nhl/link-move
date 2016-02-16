package com.nhl.link.move.runtime.jdbc;

import javax.sql.DataSource;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.configuration.server.ServerRuntimeBuilder;
import org.apache.cayenne.di.BeforeScopeEnd;

/**
 * A {@link JdbcConnector} based on externally provided DataSource.
 * 
 * @since 1.1
 */
public class DataSourceConnector implements JdbcConnector {

	private ServerRuntime runtime;
	private ObjectContext sharedContext;

	public DataSourceConnector(DataSource dataSource) {
		this.runtime = new ServerRuntimeBuilder().dataSource(dataSource).build();
		this.sharedContext = runtime.newContext();
	}

	@Override
	public ObjectContext sharedContext() {
		return sharedContext;
	}

	@BeforeScopeEnd
	@Override
	public void shutdown() {
		runtime.shutdown();
	}
}
