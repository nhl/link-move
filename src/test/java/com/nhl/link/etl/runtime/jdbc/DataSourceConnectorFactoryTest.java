package com.nhl.link.etl.runtime.jdbc;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import java.util.HashMap;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.runtime.jdbc.DataSourceConnectorFactory;

public class DataSourceConnectorFactoryTest {

	private DataSourceConnectorFactory factory;
	private DataSource ds1;
	private DataSource ds2;

	@SuppressWarnings("serial")
	@Before
	public void before() {

		ds1 = mock(DataSource.class);
		ds2 = mock(DataSource.class);

		factory = new DataSourceConnectorFactory(new HashMap<String, DataSource>() {
			{
				put("ds1", ds1);
				put("ds2", ds2);
			}
		});
	}

	@Test
	public void testConnectorDataSource() {
		assertSame(ds1, factory.connectorDataSource("ds1"));
		assertSame(ds2, factory.connectorDataSource("ds2"));
	}

	@Test(expected = EtlRuntimeException.class)
	public void testConnectorDataSource_Missing() {
		factory.connectorDataSource("ds3");
	}

}
