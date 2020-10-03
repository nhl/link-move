package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.LmRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class DataSourceConnectorFactoryTest {

	private DataSourceConnectorFactory factory;
	private DataSource ds1;
	private DataSource ds2;

	@SuppressWarnings("serial")
	@BeforeEach
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

	@Test
	public void testConnectorDataSource_Missing() {
		assertThrows(LmRuntimeException.class, () -> factory.connectorDataSource("ds3"));
	}

}
