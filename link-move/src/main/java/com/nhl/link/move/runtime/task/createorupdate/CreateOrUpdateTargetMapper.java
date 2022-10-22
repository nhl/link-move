package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Hasher;
import com.nhl.link.move.mapper.Mapper;
import org.apache.cayenne.ObjectContext;

/**
 * @since 2.6
 */
public class CreateOrUpdateTargetMapper {

    private Class<?> type;
    private Mapper mapper;

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

        return sources
                .leftJoin().on(lkm, rkm).with(targets)
                .addColumn(CreateOrUpdateSegment.TARGET_CREATED_COLUMN, r -> isCreated(r.get(CreateOrUpdateSegment.TARGET_COLUMN)))
                .convertColumn(CreateOrUpdateSegment.TARGET_COLUMN, r -> createIfMissing(r, context));
    }

    private boolean isCreated(Object v) {
        return v == null;
    }

    private Object createIfMissing(Object v, ObjectContext context) {

        // Note that "context.newObject" is an impure function. Though we don't see its undesired side effects on
        // multiple iterations due to DataFrame "materialized" feature that transparently caches the results..

        return v != null ? v : context.newObject(type);
    }
}
