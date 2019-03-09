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
		List<T> toDelete = new ArrayList<>(df.height());
		df.forEach(r -> toDelete.add((T) r.get(DeleteSegment.TARGET_COLUMN)));
		context.deleteObjects(toDelete);
	}
}
