package com.nhl.link.move.runtime;

import org.apache.cayenne.di.Injector;

/**
 * A simple wrapper around DI injector.
 *
 * @since 2.14
 */
public class DefaultLmRuntime implements LmRuntime {

    private final Injector injector;

    public DefaultLmRuntime(Injector injector) {
        this.injector = injector;
    }

    @Override
    public <T> T service(Class<T> serviceType) {
        return injector.getInstance(serviceType);
    }

    @Override
    public void shutdown() {
        injector.shutdown();
    }
}
