package com.nhl.link.etl.unit;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.conn.PoolManager;
import org.apache.cayenne.log.NoopJdbcEventLogger;
import org.apache.cayenne.query.SQLTemplate;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class DerbySrcTest {

	protected static DerbyManager derbySrc;
	protected static PoolManager srcDataSource;
	private static ServerRuntime srcRuntime;

	@SuppressWarnings("deprecation")
	@BeforeClass
	public static void startSrc() throws IOException, SQLException {
		derbySrc = new DerbyManager("target/derbysrc");

		srcRuntime = new ServerRuntime("cayenne-linketl-tests-sources.xml");

		srcDataSource = new PoolManager("org.apache.derby.jdbc.EmbeddedDriver",
				"jdbc:derby:target/derbysrc;create=true", 1, 10, "sa", "bla", NoopJdbcEventLogger.getInstance(),
				PoolManager.MAX_QUEUE_WAIT_DEFAULT);
	}

	@AfterClass
	public static void shutdownSrc() throws IOException, SQLException {

		srcRuntime.shutdown();
		srcRuntime = null;

		try {
			srcDataSource.shutdown();
		} catch (SQLException e) {
		}
		srcDataSource = null;

		derbySrc.shutdown();
		derbySrc = null;
	}

	@Before
	public void deleteSourceData() {

		ObjectContext context = srcRuntime.newContext();

		// first query in a test set will also load the schema...

		context.performGenericQuery(new SQLTemplate(Object.class, "DELETE from utest.etl1"));
		context.performGenericQuery(new SQLTemplate(Object.class, "DELETE from utest.etl3"));
		context.performGenericQuery(new SQLTemplate(Object.class, "DELETE from utest.etl2"));
		context.performGenericQuery(new SQLTemplate(Object.class, "DELETE from utest.etl4"));
		context.performGenericQuery(new SQLTemplate(Object.class, "DELETE from utest.etl5"));
	}

	protected void srcRunSql(String sql) {
		ObjectContext context = srcRuntime.newContext();
		context.performGenericQuery(new SQLTemplate(Object.class, sql));
	}

}
