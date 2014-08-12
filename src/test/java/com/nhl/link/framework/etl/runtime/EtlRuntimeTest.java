package com.nhl.link.framework.etl.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.conn.PoolManager;
import org.apache.cayenne.log.NoopJdbcEventLogger;
import org.apache.cayenne.map.DataMap;
import org.apache.cayenne.query.CapsStrategy;
import org.apache.cayenne.query.SQLSelect;
import org.apache.cayenne.query.SQLTemplate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nhl.link.framework.etl.EtlTask;
import com.nhl.link.framework.etl.Execution;
import com.nhl.link.framework.etl.IntToken;
import com.nhl.link.framework.etl.RowAttribute;
import com.nhl.link.framework.etl.SyncToken;
import com.nhl.link.framework.etl.extract.ExtractorConfig;
import com.nhl.link.framework.etl.runtime.extract.IExtractorConfigLoader;
import com.nhl.link.framework.etl.runtime.jdbc.JdbcConnector;
import com.nhl.link.framework.etl.runtime.jdbc.JdbcExtractorFactory;
import com.nhl.link.framework.etl.unit.DerbySrcTargetTest;
import com.nhl.link.framework.etl.unit.cayenne.t.Etl1t;

public class EtlRuntimeTest extends DerbySrcTargetTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(EtlRuntimeTest.class);
	private static final String FULL_EXTRACTOR = "fullE";
	private static final String RANGE_EXTRACTOR = "rangeE";

	private PoolManager dataSource;
	private JdbcConnector sourceConnector;
	private EtlRuntime runtime;
	private ObjectContext targetContext;
	private ObjectContext srcContext;

	@Before
	public void setUp() throws SQLException {

		final String connectorId = "derbysrc";

		this.dataSource = new PoolManager("org.apache.derby.jdbc.EmbeddedDriver",
				"jdbc:derby:target/derbysrc;create=true", 1, 10, "sa", "bla", NoopJdbcEventLogger.getInstance(),
				PoolManager.MAX_QUEUE_WAIT_DEFAULT);
		this.sourceConnector = new JdbcConnector(dataSource);

		this.srcContext = sourceConnector.sharedContext();
		this.targetContext = targetRuntime.newContext();

		ExtractorConfig configFull = new ExtractorConfig(FULL_EXTRACTOR);
		configFull.setType(EtlRuntimeBuilder.JDBC_EXTRACTOR_TYPE);
		configFull.setConnectorId(connectorId);
		configFull.getProperties().put(JdbcExtractorFactory.SQL_TEMPLATE_PROPERTY,
				"SELECT name, age, description FROM utest.etl1");
		configFull.getProperties().put(JdbcExtractorFactory.SQL_TEMPLATE_CAPS_PROPERTY, CapsStrategy.LOWER.name());
		configFull.setAttributes(new RowAttribute(String.class, "name", 0), new RowAttribute(Integer.class, "age", 1),
				new RowAttribute(String.class, "description", 2));

		ExtractorConfig configRange = new ExtractorConfig(RANGE_EXTRACTOR);
		configRange.setType(EtlRuntimeBuilder.JDBC_EXTRACTOR_TYPE);
		configRange.setConnectorId(connectorId);
		configRange.getProperties().put(JdbcExtractorFactory.SQL_TEMPLATE_PROPERTY,
				"SELECT name, age, description FROM utest.etl1 WHERE age > $startToken and age <= $endToken");
		configRange.getProperties().put(JdbcExtractorFactory.SQL_TEMPLATE_CAPS_PROPERTY, CapsStrategy.LOWER.name());
		configRange.setAttributes(new RowAttribute(String.class, "name", 0), new RowAttribute(Integer.class, "age", 1),
				new RowAttribute(String.class, "description", 2));

		IExtractorConfigLoader extractorConfigLoader = mock(IExtractorConfigLoader.class);
		when(extractorConfigLoader.loadConfig(FULL_EXTRACTOR)).thenReturn(configFull);
		when(extractorConfigLoader.loadConfig(RANGE_EXTRACTOR)).thenReturn(configRange);

		this.runtime = new EtlRuntimeBuilder().withConnector(connectorId, sourceConnector)
				.withTargetRuntime(targetRuntime).withExtractorConfigLoader(extractorConfigLoader).build();
	}

	@After
	public void tearDown() {

		if (dataSource != null) {
			try {
				dataSource.shutdown();
			} catch (SQLException e) {
				// ignore...
				LOGGER.warn("Error shutting down DataSource", e);
			}
		}

		if (runtime != null) {
			runtime.shutdown();
		}

	}

	@Test
	public void testFullTableSync() {

		SQLSelect<Integer> counter = SQLSelect.scalarQuery(Integer.class, "SELECT count(1) from utest.etl1t");

		DataMap dummy = srcContext.getEntityResolver().getDataMap("placeholder");

		// cleanup
		targetContext.performQuery(new SQLTemplate(Etl1t.class, "DELETE FROM utest.etl1t"));
		srcContext.performQuery(new SQLTemplate(dummy, "DELETE FROM utest.etl1", false));

		srcContext.performQuery(new SQLTemplate(dummy,
				"INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3), ('b', NULL)", false));

		EtlTask task = runtime.getTaskService().createTaskBuilder(Etl1t.class).withExtractor(FULL_EXTRACTOR)
				.matchBy(new RowAttribute(String.class, Etl1t.NAME.getName(), 0)).task();

		Execution e1 = task.run(SyncToken.nullToken());
		LOGGER.info(e1.toString());

		assertEquals(2, e1.getExtracted());
		assertEquals(2, e1.getCreated());
		assertEquals(0, e1.getUpdated());
		assertEquals(2, counter.selectOne(targetContext).intValue());
		assertNull(SQLSelect.scalarQuery(Integer.class, "SELECT AGE from utest.etl1t WHERE NAME = 'b'").selectOne(
				targetContext));

		srcContext.performQuery(new SQLTemplate(dummy, "INSERT INTO utest.etl1 (NAME) VALUES ('c')", false));
		srcContext.performQuery(new SQLTemplate(dummy, "UPDATE utest.etl1 SET AGE = 5 WHERE NAME = 'a'", false));

		Execution e2 = task.run(SyncToken.nullToken());
		LOGGER.info(e2.toString());

		assertEquals(3, e2.getExtracted());
		assertEquals(1, e2.getCreated());
		assertEquals(1, e2.getUpdated());

		assertEquals(3, counter.selectOne(targetContext).intValue());
		assertEquals(
				5,
				SQLSelect.scalarQuery(Integer.class, "SELECT AGE from utest.etl1t WHERE NAME = 'a'")
						.selectOne(targetContext).intValue());

		srcContext.performQuery(new SQLTemplate(dummy, "DELETE FROM utest.etl1 WHERE NAME = 'a'", false));

		Execution e3 = task.run(SyncToken.nullToken());
		LOGGER.info(e3.toString());

		assertEquals(2, e3.getExtracted());
		assertEquals(0, e3.getCreated());
		assertEquals(0, e3.getUpdated());
		assertEquals(3, counter.selectOne(targetContext).intValue());

		Execution e4 = task.run(SyncToken.nullToken());
		LOGGER.info(e4.toString());

		assertEquals(2, e4.getExtracted());
		assertEquals(0, e4.getCreated());
		assertEquals(0, e4.getUpdated());
	}

	@Test
	public void testIncrementalTableSync() {

		SQLSelect<Integer> counter = SQLSelect.scalarQuery(Integer.class, "SELECT count(1) from utest.etl1t");

		DataMap dummy = srcContext.getEntityResolver().getDataMap("placeholder");

		// cleanup
		targetContext.performQuery(new SQLTemplate(Etl1t.class, "DELETE FROM utest.etl1t"));
		srcContext.performQuery(new SQLTemplate(dummy, "DELETE FROM utest.etl1", false));

		srcContext.performQuery(new SQLTemplate(dummy, "INSERT INTO utest.etl1 (NAME, AGE) VALUES ('a', 3), ('b', 1)",
				false));

		EtlTask task = runtime.getTaskService().createTaskBuilder(Etl1t.class).withExtractor(RANGE_EXTRACTOR)
				.matchBy(new RowAttribute(String.class, Etl1t.NAME.getName(), 0)).task();

		Execution e1 = task.run(new IntToken("testIncrementalTableSync", 2));
		LOGGER.info(e1.toString());

		assertEquals(1, counter.selectOne(targetContext).intValue());

		Execution e2 = task.run(new IntToken("testIncrementalTableSync", 5));
		LOGGER.info(e2.toString());

		assertEquals(2, counter.selectOne(targetContext).intValue());
	}

}
