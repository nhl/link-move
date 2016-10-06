package com.nhl.link.move.runtime;

import com.nhl.link.move.runtime.task.ITaskService;

/**
 * The main object to access ETL framework services.
 */
public interface LmRuntime {

	/**
	 * @since 2.1
     */
	<T> T service(Class<T> serviceType);

	default ITaskService getTaskService() {
		return service(ITaskService.class);
	}
	
	void shutdown();
}
