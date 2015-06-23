package com.nhl.link.move.unit;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.datasource.DataSourceBuilder;
import org.apache.cayenne.datasource.PoolingDataSource;
import org.apache.cayenne.query.SQLSelect;
import org.apache.cayenne.query.SQLTemplate;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class DerbySrcTest {

	protected static CayenneDerbyStack srcStack;
	protected static PoolingDataSource srcDataSource;

	@BeforeClass
	public static void startSrc() throws IOException, SQLException {
		srcStack = new CayenneDerbyStack("derbysrc", "cayenne-linketl-tests-sources.xml");
		srcDataSource = DataSourceBuilder.url("jdbc:derby:" + srcStack.getDerbyPath() + ";create=true")
				.driver("org.apache.derby.jdbc.EmbeddedDriver").userName("sa").pool(1, 10).build();
	}

	@AfterClass
	public static void shutdownSrc() throws IOException, SQLException {

		srcStack.shutdown();

		try {
			srcDataSource.close();
		} catch (Exception e) {
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
