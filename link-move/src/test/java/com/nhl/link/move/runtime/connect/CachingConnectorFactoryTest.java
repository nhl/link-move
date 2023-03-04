package com.nhl.link.move.runtime.connect;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.Connector;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

public class CachingConnectorFactoryTest {

    @Test
    public void testCreateConnector() {

        Connector[] connectors1 = new Connector[]{mock(Connector.class), mock(Connector.class)};
        Connector[] connectors2 = new Connector[]{mock(Connector.class), mock(Connector.class)};

        IConnectorFactory<Connector> nonCaching = new IConnectorFactory<>() {
            int i1;
            int i2;

            @Override
            public Class<Connector> getConnectorType() {
                return Connector.class;
            }

            @Override
            public Optional<? extends Connector> createConnector(String id) throws LmRuntimeException {

                switch (id) {
                    case "id1":
                        return Optional.of(connectors1[i1++]);
                    case "id2":
                        return Optional.of(connectors2[i2++]);
                }
                return Optional.empty();
            }
        };

        CachingConnectorFactory<Connector> caching = new CachingConnectorFactory<>(nonCaching);
        assertSame(connectors1[0], caching.createConnector("id1").get());
        assertSame(connectors1[0], caching.createConnector("id1").get());
        assertSame(connectors2[0], caching.createConnector("id2").get());
        assertSame(connectors2[0], caching.createConnector("id2").get());
        assertSame(connectors1[0], caching.createConnector("id1").get());
    }
}
