package com.nhl.link.move.runtime.connect;

import com.nhl.link.move.connect.Connector;
import org.apache.cayenne.di.DIRuntimeException;
import org.apache.cayenne.di.Inject;
import org.apache.cayenne.di.Provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @since 3.0
 */
public class ConnectorServiceProvider implements Provider<IConnectorService> {

    private final List<IConnectorFactory> factories;

    public ConnectorServiceProvider(@Inject List<IConnectorFactory> factories) {
        this.factories = factories;
    }

    @Override
    public IConnectorService get() throws DIRuntimeException {
        // ugly - intentionally hiding generics in the intermediate collection
        Map<Class, List> byType = new HashMap<>();
        factories.forEach(f -> byType.computeIfAbsent(f.getConnectorType(), t -> new ArrayList<>(3)).add(f));

        Map<Class<? extends Connector>, IConnectorFactory<?>> byTypeComposed = new HashMap<>();
        byType.forEach((k, v) -> byTypeComposed.put(k, composeConnector(k, v)));

        return new ConnectorService(byTypeComposed);
    }

    private <C extends Connector> IConnectorFactory composeConnector(Class<C> type, List<IConnectorFactory<C>> connectors) {
        IConnectorFactory<C> composed = connectors.size() == 1
                ? connectors.get(0)
                : new ChainConnectorFactory<>(type, connectors);

        return new CachingConnectorFactory<>(composed);
    }
}
