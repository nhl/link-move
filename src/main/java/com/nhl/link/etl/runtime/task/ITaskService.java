package com.nhl.link.etl.runtime.task;

import org.apache.cayenne.DataObject;

public interface ITaskService {

	<T extends DataObject> MatchingTaskBuilder<T> createTaskBuilder(Class<T> type);

}
