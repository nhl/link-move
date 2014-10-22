package com.nhl.link.etl.runtime.cayenne;

import org.apache.cayenne.di.Inject;

import com.nhl.link.etl.runtime.jdbc.DataSourceConnectorFactory;

/**
 * @since 1.1
 */
public class TargetConnectorFactory extends DataSourceConnectorFactory {

	public TargetConnectorFactory(@Inject ITargetCayenneService cayenneService) {
		super(cayenneService.dataSources());
	}

}
