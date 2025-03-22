package com.nhl.link.move.runtime.task.delete;

import org.dflib.DataFrame;
import com.nhl.link.move.mapper.Mapper;
import org.dflib.Exp;

/**
 * @since 1.3
 */
public class TargetMapper {

    private final Mapper mapper;

    public TargetMapper(Mapper mapper) {
        this.mapper = mapper;
    }

    public DataFrame map(DataFrame df) {

        // TODO: report dupes?

        return df
                .colsAppend(DeleteSegment.KEY_COLUMN)
                .merge(Exp.$col(DeleteSegment.TARGET_COLUMN).mapVal(mapper::keyForTarget));
    }
}
