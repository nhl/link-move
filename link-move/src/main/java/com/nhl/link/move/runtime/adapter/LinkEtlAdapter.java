package com.nhl.link.move.runtime.adapter;

import com.nhl.link.move.runtime.LmAdapter;
import org.apache.cayenne.di.Binder;

/**
 * @deprecated in favor of {@link LmAdapter}
 */
@Deprecated(since = "3.0.0", forRemoval = true)
public interface LinkEtlAdapter extends LmAdapter {

    void contributeToRuntime(Binder binder);

    @Override
    default void configure(Binder binder) {
        contributeToRuntime(binder);
    }
}
