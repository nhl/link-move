package com.nhl.link.move.runtime.task.delete;

import com.nhl.dflib.DataFrame;
import com.nhl.link.move.mapper.Mapper;
import org.apache.cayenne.DataObject;

/**
 * @since 1.3
 */
public class TargetMapper<T extends DataObject> {

    private Mapper mapper;

    public TargetMapper(Mapper mapper) {
        this.mapper = mapper;
    }

    public DataFrame map(DataFrame df) {

        // TODO: report dupes?

        return df.addColumn(
                DeleteSegment.KEY_COLUMN,
                r -> mapper.keyForTarget((T) r.get(DeleteSegment.TARGET_COLUMN)));
    }
}
