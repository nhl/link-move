package com.nhl.link.move.runtime.task.delete;

import com.nhl.dflib.DataFrame;
import org.apache.cayenne.ObjectContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @since 1.3
 */
public class DeleteTargetStage<T> {

	public void delete(ObjectContext context, DataFrame df) {
		List<T> toDelete = new ArrayList<>((int) df.height());
		df.consume((c, r) -> toDelete.add((T) c.get(r, DeleteSegment.TARGET_COLUMN)));
		context.deleteObjects(toDelete);
	}
}
