package com.nhl.link.move.runtime.cayenne;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.dba.DbAdapter;
import org.apache.cayenne.map.EntityResolver;

import javax.sql.DataSource;
import java.util.Map;

public interface ITargetCayenneService {

	/**
	 * Returns a map of DataSources available in Cayenne runtime
	 * 
	 * @since 1.1
	 */
	Map<String, DataSource> dataSources();

	ObjectContext newContext();

	EntityResolver entityResolver();

	/**
	 * @since 3.0.0
	 */
	DbAdapter dbAdapter(String dataMapName);
}
