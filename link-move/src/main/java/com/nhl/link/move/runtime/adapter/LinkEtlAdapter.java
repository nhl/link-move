package com.nhl.link.move.runtime.adapter;

import org.apache.cayenne.di.Binder;

/**
 * @since 1.1
 */
public interface LinkEtlAdapter {

	/**
	 * Registers new and/or overrides existing DI services in the ETL runtime.
	 */
	void contributeToRuntime(Binder binder);
}
