package com.nhl.link.move.runtime.jdbc;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.configuration.server.ServerRuntimeBuilder;
import org.apache.cayenne.di.BeforeScopeEnd;
import org.apache.cayenne.java8.CayenneJava8Module;

import javax.sql.DataSource;

/**
 * A {@link JdbcConnector} based on externally provided DataSource.
 * 
 * @since 1.1
 */
public class DataSourceConnector implements JdbcConnector {

	private ServerRuntime runtime;
	private ObjectContext sharedContext;

	public DataSourceConnector(DataSource dataSource) {
		this("dsconnector-" + System.nanoTime(), dataSource);
	}

	/**
	 * @since 1.7
	 */
	public DataSourceConnector(String name, DataSource dataSource) {

		// assigning explicit name to the Cayenne runtime to avoid transaction
		// conflicts for similarly named DataNodes between this runtime and
		// other Cayenne stacks present in the system.
		this.runtime = new ServerRuntimeBuilder(name)
				.addModule(new CayenneJava8Module())
				.dataSource(dataSource)
				.build();
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
