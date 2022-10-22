package com.nhl.link.move.runtime.task.delete;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Series;
import org.apache.cayenne.ObjectContext;

/**
 * @since 1.3
 */
public class DeleteTargetStage {

	public void delete(ObjectContext context, DataFrame df) {
		Series<?> toDelete = df.getColumn(DeleteSegment.TARGET_COLUMN);
		context.deleteObjects(toDelete.toList());
	}
}
