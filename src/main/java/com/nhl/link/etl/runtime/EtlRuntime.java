package com.nhl.link.etl.runtime;

import com.nhl.link.etl.runtime.task.ITaskService;

/**
 * The main object to access ETL framework services.
 */
public interface EtlRuntime {

	ITaskService getTaskService();
	
	void shutdown();
}
