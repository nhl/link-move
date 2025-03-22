package com.nhl.link.move.runtime;

import org.apache.cayenne.di.Binder;

/**
 * A callback interface that allows users to customize LinkMove DI stack during LM runtime bootstrap.
 *
 * @since 3.0.0
 */
public interface LmAdapter {

    /**
     * Registers new and/or overrides existing DI services in the LM runtime.
     */
    void configure(Binder binder);
}
