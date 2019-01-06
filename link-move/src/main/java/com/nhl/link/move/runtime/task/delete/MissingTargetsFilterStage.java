package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.df.DataFrame;

import java.util.Set;

/**
 * @since 1.3
 */
public class MissingTargetsFilterStage<T> {

    public DataFrame filterMissing(
            DataFrame mappedTargets,
            Set<Object> sourceKeys) {

        return mappedTargets.filter(
                (c, r) -> !sourceKeys.contains(c.get(r, DeleteSegment.KEY_COLUMN)));
    }
}
