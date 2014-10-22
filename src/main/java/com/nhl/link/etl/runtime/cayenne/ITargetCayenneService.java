package com.nhl.link.etl.runtime.cayenne;

import java.util.Map;

import javax.sql.DataSource;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.map.EntityResolver;

public interface ITargetCayenneService {

	/**
	 * Returns a map of DataSources available in Cayenne runtime
	 * 
	 * @since 1.1
	 */
	Map<String, DataSource> dataSources();

	ObjectContext newContext();

	EntityResolver entityResolver();
}
