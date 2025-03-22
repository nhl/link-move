package com.nhl.link.move.runtime.task.createorupdate;

import org.dflib.DataFrame;
import com.nhl.link.move.mapper.Mapper;

/**
 * @since 1.3
 */
public class SourceMapper {

    private Mapper mapper;

    public SourceMapper(Mapper mapper) {
        this.mapper = mapper;
    }

    public DataFrame map(DataFrame df) {
        return df
                .colsAppend(CreateOrUpdateSegment.KEY_COLUMN)
                .merge(mapper::keyForSource);
    }
}
