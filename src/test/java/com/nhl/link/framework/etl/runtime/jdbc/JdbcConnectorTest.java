package com.nhl.link.framework.etl.runtime.jdbc;

import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.conn.PoolManager;
import org.apache.cayenne.log.NoopJdbcEventLogger;
import org.apache.cayenne.map.DataMap;
import org.apache.cayenne.query.SQLTemplate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.framework.etl.unit.DerbySrcTest;

public class JdbcConnectorTest extends DerbySrcTest {

	private JdbcConnector connector;
	private PoolManager dataSource;

	@Before
	public void setUp() throws SQLException {
		this.dataSource = new PoolManager("org.apache.derby.jdbc.EmbeddedDriver",
				"jdbc:derby:target/derbysrc;create=true", 1, 10, "sa", "bla", NoopJdbcEventLogger.getInstance(),
				PoolManager.MAX_QUEUE_WAIT_DEFAULT);

		this.connector = new JdbcConnector(dataSource);
	}

	@After
	public void tearDown() throws SQLException {
		connector.shutdown();

		if (dataSource != null) {
			dataSource.shutdown();
		}
	}

	@Test
	public void testSharedContext() {
		ObjectContext context = connector.sharedContext();
		assertNotNull(context);

		DataMap dummy = context.getEntityResolver().getDataMap("placeholder");

		context.performQuery(new SQLTemplate(dummy, "INSERT INTO utest.etl1 (NAME) VALUES ('a')", false));
	}
}
