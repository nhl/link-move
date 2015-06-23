package com.nhl.link.move.runtime;

import com.nhl.link.move.runtime.task.ITaskService;

/**
 * The main object to access ETL framework services.
 */
public interface EtlRuntime {

	ITaskService getTaskService();
	
	void shutdown();
}
