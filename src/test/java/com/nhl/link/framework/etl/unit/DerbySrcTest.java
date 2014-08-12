package com.nhl.link.framework.etl.unit;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.query.SQLSelect;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class DerbySrcTest {

	protected static DerbyManager derbySrc;

	@BeforeClass
	public static void setUpSrc() throws IOException, SQLException {
		derbySrc = new DerbyManager("target/derbysrc");

		// create and then shutdown the runtime - this will load the schema
		ServerRuntime runtime = new ServerRuntime("cayenne-linketl-tests-sources.xml");
		runtime.newContext().performQuery(SQLSelect.dataRowQuery("SELECT * FROM utest.etl1"));
		runtime.shutdown();
	}

	@AfterClass
	public static void tearDownSrc() throws IOException, SQLException {

		derbySrc.shutdown();
		derbySrc = null;
	}

}
