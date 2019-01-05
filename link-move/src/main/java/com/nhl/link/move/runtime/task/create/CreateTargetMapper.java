package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.df.DataFrame;
import com.nhl.link.move.df.Index;
import com.nhl.link.move.df.map.MapContext;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

/**
 * @param <T>
 * @since 2.6
 */
public class CreateTargetMapper<T extends DataObject> {

    private Class<T> type;

    public CreateTargetMapper(Class<T> type) {
        this.type = type;
    }

    public DataFrame map(ObjectContext cayenneContext, DataFrame sources) {
        Index newColumns = sources.getColumns().addNames(CreateSegment.TARGET_COLUMN, CreateSegment.TARGET_CREATED_COLUMN);
        return sources.map(newColumns, (c, r) -> mapRow(c, r, cayenneContext));
    }

    protected Object[] mapRow(MapContext context, Object[] source, ObjectContext cayenneContext) {
        Object[] target = context.copyToTarget(source);

        context.set(target, CreateSegment.TARGET_COLUMN, create(cayenneContext));
        context.set(target, CreateSegment.TARGET_CREATED_COLUMN, true);

        return target;
    }

    protected T create(ObjectContext context) {

        // Note that "context.newObject" is an impure function. Though we don't see its undesired side effects on
        // multiple iterations due to DataFrame "materialized" feature that transparently caches the results..

        return context.newObject(type);
    }
}
