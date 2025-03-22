package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.mapper.Mapper;
import org.apache.cayenne.ObjectContext;
import org.dflib.DataFrame;
import org.dflib.Hasher;

import static org.dflib.Exp.*;

/**
 * @since 2.6
 */
public class CreateOrUpdateTargetMapper {

    private final Class<?> type;
    private final Mapper mapper;

    public CreateOrUpdateTargetMapper(Class<?> type, Mapper mapper) {
        this.mapper = mapper;
        this.type = type;
    }

    public DataFrame map(
            ObjectContext context,
            DataFrame sources,
            DataFrame targets) {

        Hasher lkm = r -> r.get(CreateOrUpdateSegment.KEY_COLUMN);
        Hasher rkm = r -> mapper.keyForTarget(r.get(CreateOrUpdateSegment.TARGET_COLUMN));

        DataFrame df = sources.leftJoin(targets).on(lkm, rkm).select();

        return df
                .colsAppend(CreateOrUpdateSegment.TARGET_CREATED_COLUMN).merge($col(CreateOrUpdateSegment.TARGET_COLUMN).isNull())

                .cols(CreateOrUpdateSegment.TARGET_COLUMN).merge(ifExp(
                        $bool(CreateOrUpdateSegment.TARGET_CREATED_COLUMN),
                        $val(type).mapVal(context::newObject),
                        $col(CreateOrUpdateSegment.TARGET_COLUMN)
                ));
    }

    private boolean isCreated(Object v) {
        return v == null;
    }
}
