package com.nhl.link.move.unit;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;

import com.nhl.link.move.Execution;
import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.runtime.LmRuntime;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import com.nhl.link.move.runtime.connect.URIConnectorFactory;
import com.nhl.link.move.runtime.jdbc.DataSourceConnector;

public abstract class LmIntegrationTest extends DerbySrcTargetTest {

	protected LmRuntime etl;

	@Before
	public void before() {
		this.etl = createEtl();
	}

	@After
	public void shutdown() {
		if (etl != null) {
			etl.shutdown();
		}
	}

	protected LmRuntime createEtl() {
		Connector c = new DataSourceConnector(srcDataSource);
		return new LmRuntimeBuilder().withConnector("derbysrc", c).withTargetRuntime(targetStack.runtime())
				.withConnectorFactory(StreamConnector.class, new URIConnectorFactory())
				.build();
	}

	protected void assertExec(int extracted, int created, int updated, int deleted, Execution exec) {
		assertEquals("Extracted unexpected number of records.", extracted, exec.getStats().getExtracted());
		assertEquals("Created unexpected number of records.", created, exec.getStats().getCreated());
		assertEquals("Updated unexpected number of records.", updated, exec.getStats().getUpdated());
		assertEquals("Deleted unexpected number of records.", deleted, exec.getStats().getDeleted());

	}
}
