package com.nhl.link.move.runtime.task.delete;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Exp;

import java.util.Set;

/**
 * @since 1.3
 */
public class MissingTargetsFilterStage {

    public DataFrame filterMissing(
            DataFrame mappedTargets,
            Set<Object> sourceKeys) {

        return mappedTargets.selectRows(
                Exp.$col(DeleteSegment.KEY_COLUMN).mapConditionVal(v -> !sourceKeys.contains(v)));
    }
}
