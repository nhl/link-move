package com.nhl.link.etl.runtime.jdbc;

import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.map.DataMap;
import org.apache.cayenne.query.SQLTemplate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.etl.unit.DerbySrcTest;

public class JdbcConnectorTest extends DerbySrcTest {

	private JdbcConnector connector;

	@Before
	public void setUp() throws SQLException {
		this.connector = new JdbcConnector(srcDataSource);
	}

	@After
	public void tearDown() throws SQLException {
		connector.shutdown();
	}

	@Test
	public void testSharedContext() {
		ObjectContext context = connector.sharedContext();
		assertNotNull(context);

		DataMap dummy = context.getEntityResolver().getDataMap("placeholder");

		context.performQuery(new SQLTemplate(dummy, "INSERT INTO utest.etl1 (NAME) VALUES ('a')", false));
	}
}
