package com.nhl.link.etl.runtime.task.delete;

import java.util.List;

import org.apache.cayenne.ObjectContext;

/**
 * @since 1.3
 */
public class DeleteTargetStage<T> {

	public void delete(ObjectContext context, List<T> missingTargets) {
		context.deleteObjects(missingTargets);
	}
}
