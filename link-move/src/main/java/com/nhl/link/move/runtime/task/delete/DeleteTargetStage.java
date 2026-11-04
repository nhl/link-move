package com.nhl.link.move.runtime.task.delete;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.Persistent;
import org.dflib.DataFrame;
import org.dflib.Series;

/**
 * @since 1.3
 */
public class DeleteTargetStage {

	public void delete(ObjectContext context, DataFrame df) {
		// "unsafe" cast, as the column type is often inferred as Object, while the elements are Persistent
		Series<Persistent> toDelete = df.getColumn(DeleteSegment.TARGET_COLUMN).unsafeCastAs(Persistent.class);
		context.deleteObjects(toDelete.toList());
	}
}
