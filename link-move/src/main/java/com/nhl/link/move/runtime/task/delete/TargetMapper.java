package com.nhl.link.move.runtime.task.delete;

import com.nhl.dflib.DataFrame;
import com.nhl.link.move.mapper.Mapper;

/**
 * @since 1.3
 */
public class TargetMapper {

    private Mapper mapper;

    public TargetMapper(Mapper mapper) {
        this.mapper = mapper;
    }

    public DataFrame map(DataFrame df) {

        // TODO: report dupes?

        return df.addColumn(
                DeleteSegment.KEY_COLUMN,
                r -> mapper.keyForTarget(r.get(DeleteSegment.TARGET_COLUMN)));
    }
}
