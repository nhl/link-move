package com.nhl.link.move.runtime.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

public class DataSourceConnectorFactoryTest {

	private DataSourceConnectorFactory factory;
	private DataSource ds1;
	private DataSource ds2;

	@BeforeEach
	public void before() {
		this.ds1 = mock(DataSource.class);
		this.ds2 = mock(DataSource.class);
		this.factory = new DataSourceConnectorFactory(Map.of("ds1", ds1, "ds2", ds2));
	}

	@Test
	public void connectorDataSource() {
		assertSame(ds1, factory.connectorDataSource("ds1").get());
		assertSame(ds2, factory.connectorDataSource("ds2").get());
	}

	@Test
	public void connectorDataSource_Missing() {
		assertFalse(factory.connectorDataSource("ds3").isPresent());
	}

}
