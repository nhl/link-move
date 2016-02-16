package com.nhl.link.move.runtime.connect;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.runtime.connect.ConnectorService;
import com.nhl.link.move.runtime.connect.IConnectorFactory;
import com.nhl.link.move.runtime.jdbc.JdbcConnector;

public class ConnectorServiceTest {

	private ConnectorService connectorService;
	private JdbcConnector connectorA;
	private JdbcConnector connectorB;

	@SuppressWarnings("serial")
	@Before
	public void before() {
		connectorA = mock(JdbcConnector.class);
		connectorB = mock(JdbcConnector.class);

		connectorService = new ConnectorService(new HashMap<String, IConnectorFactory<?>>() {
			{
				@SuppressWarnings("unchecked")
				IConnectorFactory<JdbcConnector> bFactory = mock(IConnectorFactory.class);

				// this connector will never be reachable, as we explicitly
				// provide a connector for "a" ID
				when(bFactory.createConnector("a")).thenReturn(mock(JdbcConnector.class));

				// this connector will be provided by the factory
				when(bFactory.createConnector("b")).thenReturn(connectorB);
				put(JdbcConnector.class.getName(), bFactory);
			}
		}, new HashMap<String, Connector>() {
			{
				put("a", connectorA);
			}
		});
	}

	@Test
	public void testGetConnector() {
		assertSame(connectorA, connectorService.getConnector(JdbcConnector.class, "a"));
		assertSame(connectorB, connectorService.getConnector(JdbcConnector.class, "b"));
		assertSame(connectorA, connectorService.getConnector(JdbcConnector.class, "a"));
		assertSame(connectorB, connectorService.getConnector(JdbcConnector.class, "b"));
	}
}
