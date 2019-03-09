package com.nhl.link.move.runtime.task.delete;

import com.nhl.dflib.DataFrame;

import java.util.Set;

/**
 * @since 1.3
 */
public class MissingTargetsFilterStage<T> {

    public DataFrame filterMissing(
            DataFrame mappedTargets,
            Set<Object> sourceKeys) {

        return mappedTargets.filter(r -> !sourceKeys.contains(r.get(DeleteSegment.KEY_COLUMN)));
    }
}
