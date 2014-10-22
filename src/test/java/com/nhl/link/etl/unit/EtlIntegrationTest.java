package com.nhl.link.etl.unit;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;

import com.nhl.link.etl.Execution;
import com.nhl.link.etl.connect.Connector;
import com.nhl.link.etl.runtime.EtlRuntime;
import com.nhl.link.etl.runtime.EtlRuntimeBuilder;
import com.nhl.link.etl.runtime.jdbc.JdbcConnector;

public abstract class EtlIntegrationTest extends DerbySrcTargetTest {

	protected EtlRuntime etl;

	@Before
	public void before() {
		Connector c = new JdbcConnector(srcDataSource);
		this.etl = new EtlRuntimeBuilder().withConnector("derbysrc", c).withTargetRuntime(targetStack.runtime())
				.build();
	}

	@After
	public void shutdown() {
		etl.shutdown();
	}

	protected void assertExec(int extracted, int created, int updated, Execution exec) {
		assertEquals(extracted, exec.getExtracted());
		assertEquals(created, exec.getCreated());
		assertEquals(updated, exec.getUpdated());
	}
}
