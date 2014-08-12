package com.nhl.link.framework.etl.runtime;

import com.nhl.link.framework.etl.runtime.task.ITaskService;

/**
 * The main object to access ETL framework services.
 */
public interface EtlRuntime {

	ITaskService getTaskService();
	
	void shutdown();
}
