package com.nhl.link.etl.runtime.cayenne;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.map.EntityResolver;

public class TargetCayenneService implements ITargetCayenneService {

	private ServerRuntime runtime;

	public TargetCayenneService(ServerRuntime runtime) {
		this.runtime = runtime;
	}

	@Override
	public ObjectContext newContext() {
		return runtime.newContext();
	}
	
	@Override
	public EntityResolver entityResolver() {
		return runtime.getChannel().getEntityResolver();
	}
}
