package com.nhl.link.move.runtime.connect;

import com.nhl.link.move.runtime.jdbc.JdbcConnector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

public class ChainConnectorFactoryTest {

    private ChainConnectorFactory<JdbcConnector> connectorFactory;
    private JdbcConnector connectorA;
    private JdbcConnector connectorB;

    @BeforeEach
    public void before() {
        this.connectorA = mock(JdbcConnector.class);
        this.connectorB = mock(JdbcConnector.class);

        List<IConnectorFactory<JdbcConnector>> connectors = List.of(
                new ConnectorInstanceFactory<>(JdbcConnector.class, "a", connectorA),
                new ConnectorInstanceFactory<>(JdbcConnector.class, "b", connectorB)
        );
        this.connectorFactory = new ChainConnectorFactory<>(JdbcConnector.class, connectors);
    }

    @Test
    public void testCreateConnector() {
        assertSame(connectorA, connectorFactory.createConnector("a").get());
        assertSame(connectorB, connectorFactory.createConnector("b").get());
        assertSame(connectorA, connectorFactory.createConnector("a").get());
        assertSame(connectorB, connectorFactory.createConnector("b").get());
    }
}
