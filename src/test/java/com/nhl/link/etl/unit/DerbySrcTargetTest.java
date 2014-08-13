package com.nhl.link.etl.unit;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.query.SQLSelect;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class DerbySrcTargetTest extends DerbySrcTest {

	protected static DerbyManager derbyTarget;
	protected static ServerRuntime targetRuntime;

	@BeforeClass
	public static void setUpTarget() throws IOException, SQLException {
		derbyTarget = new DerbyManager("target/derbytarget");

		targetRuntime = new ServerRuntime("cayenne-linketl-tests-targets.xml");

		// this will load the schema
		targetRuntime.newContext().performQuery(SQLSelect.dataRowQuery("SELECT * FROM utest.etl1t"));
	}

	@AfterClass
	public static void tearDownTarget() throws IOException, SQLException {

		targetRuntime.shutdown();

		derbyTarget.shutdown();
		derbyTarget = null;
	}

}
