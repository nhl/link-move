package com.nhl.link.etl.runtime.cayenne;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.map.EntityResolver;

public interface ITargetCayenneService {

	ObjectContext newContext();

	/**
	 * @since 6.14
	 */
	EntityResolver entityResolver();
}
