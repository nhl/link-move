package com.nhl.link.move.unit;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.configuration.server.ServerRuntime;

public class CayenneDerbyStack {

	private String derbyPath;
	private DerbyManager derby;
	private ServerRuntime runtime;

	public CayenneDerbyStack(String derbyName, String cayenneProject) {
		derbyPath = "target/" + derbyName;
		derby = new DerbyManager(derbyPath);
		runtime = new ServerRuntime(cayenneProject);
	}

	public void shutdown() {
		runtime.shutdown();
		derby.shutdown();
	}

	public String getDerbyPath() {
		return derbyPath;
	}

	public ObjectContext newContext() {
		return runtime.newContext();
	}

	public ServerRuntime runtime() {
		return runtime;
	}
}
