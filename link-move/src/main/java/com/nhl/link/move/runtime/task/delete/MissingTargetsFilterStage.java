package com.nhl.link.move.runtime.task.delete;

import org.dflib.DataFrame;

import java.util.Set;

import static org.dflib.Exp.$col;

/**
 * @since 1.3
 */
public class MissingTargetsFilterStage {

    public DataFrame filterMissing(
            DataFrame mappedTargets,
            Set<Object> sourceKeys) {

        return mappedTargets
                .rows($col(DeleteSegment.KEY_COLUMN).mapBoolVal(v -> !sourceKeys.contains(v)))
                .select();
    }
}
