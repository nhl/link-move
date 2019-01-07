package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.yadf.DataFrame;
import com.nhl.yadf.join.IndexedJoiner;
import com.nhl.yadf.join.JoinSemantics;
import com.nhl.link.move.mapper.Mapper;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

/**
 * @since 2.6
 */
public class TargetMapper<T extends DataObject> {

    private Class<T> type;
    private Mapper mapper;

    public TargetMapper(Class<T> type, Mapper mapper) {
        this.mapper = mapper;
        this.type = type;
    }

    public DataFrame map(
            ObjectContext context,
            DataFrame sources,
            DataFrame targets) {

        IndexedJoiner<Object> joiner = new IndexedJoiner<>(
                (c, r) -> c.get(r, CreateOrUpdateSegment.KEY_COLUMN),
                (c, r) -> mapper.keyForTarget((DataObject) c.get(r, CreateOrUpdateSegment.TARGET_COLUMN)),
                JoinSemantics.left);

        return sources
                .join(targets, joiner)
                .addColumn(CreateOrUpdateSegment.TARGET_CREATED_COLUMN, (c, r) -> isCreated(c.get(r, CreateOrUpdateSegment.TARGET_COLUMN)))
                .mapColumn(CreateOrUpdateSegment.TARGET_COLUMN, (c, r) -> createIfMissing(c.get(r, CreateOrUpdateSegment.TARGET_COLUMN), context));
    }

    private boolean isCreated(Object v) {
        return v == null;
    }

    private T createIfMissing(Object v, ObjectContext context) {

        // Note that "context.newObject" is an impure function. Though we don't see its undesired side effects on
        // multiple iterations due to DataFrame "materialized" feature that transparently caches the results..

        return v != null ? (T) v : context.newObject(type);
    }
}
