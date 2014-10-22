package com.nhl.link.etl.unit;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.conn.PoolManager;
import org.apache.cayenne.log.NoopJdbcEventLogger;
import org.apache.cayenne.query.SQLSelect;
import org.apache.cayenne.query.SQLTemplate;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class DerbySrcTest {

	protected static CayenneDerbyStack srcStack;
	protected static PoolManager srcDataSource;

	@SuppressWarnings("deprecation")
	@BeforeClass
	public static void startSrc() throws IOException, SQLException {
		srcStack = new CayenneDerbyStack("derbysrc", "cayenne-linketl-tests-sources.xml");
		srcDataSource = new PoolManager("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:" + srcStack.getDerbyPath()
				+ ";create=true", 1, 10, "sa", "bla", NoopJdbcEventLogger.getInstance(),
				PoolManager.MAX_QUEUE_WAIT_DEFAULT);
	}

	@AfterClass
	public static void shutdownSrc() throws IOException, SQLException {

		srcStack.shutdown();

		try {
			srcDataSource.shutdown();
		} catch (SQLException e) {
		}
		srcDataSource = null;
	}

	@Before
	public void deleteSourceData() {

		ObjectContext context = srcStack.newContext();

		// first query in a test set will also load the schema...

		context.performGenericQuery(new SQLTemplate(Object.class, "DELETE from utest.etl1"));
		context.performGenericQuery(new SQLTemplate(Object.class, "DELETE from utest.etl3"));
		context.performGenericQuery(new SQLTemplate(Object.class, "DELETE from utest.etl2"));
		context.performGenericQuery(new SQLTemplate(Object.class, "DELETE from utest.etl4"));
		context.performGenericQuery(new SQLTemplate(Object.class, "DELETE from utest.etl5"));
	}

	protected void srcRunSql(String sql) {
		ObjectContext context = srcStack.newContext();
		context.performGenericQuery(new SQLTemplate(Object.class, sql));
	}

	protected int srcScalar(String sql) {
		ObjectContext context = srcStack.newContext();
		SQLSelect<Integer> query = SQLSelect.scalarQuery(Integer.class, sql);
		return query.selectOne(context).intValue();
	}
}
