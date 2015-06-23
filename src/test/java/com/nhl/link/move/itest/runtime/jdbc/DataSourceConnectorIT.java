package com.nhl.link.move.itest.runtime.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.map.DataMap;
import org.apache.cayenne.query.SQLTemplate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.move.runtime.jdbc.DataSourceConnector;
import com.nhl.link.move.unit.DerbySrcTest;

public class DataSourceConnectorIT extends DerbySrcTest {

	private DataSourceConnector connector;

	@Before
	public void setUp() throws SQLException {
		this.connector = new DataSourceConnector(srcDataSource);
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
		assertEquals(1, srcScalar("SELECT count(1) from utest.etl1"));
	}

}
