package com.nhl.link.move.runtime.task.create;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.row.RowBuilder;
import com.nhl.dflib.row.RowProxy;
import org.apache.cayenne.ObjectContext;

/**
 * @since 2.6
 */
public class CreateTargetMapper {

    private final Class<?> type;

    public CreateTargetMapper(Class<?> type) {
        this.type = type;
    }

    public DataFrame map(ObjectContext cayenneContext, DataFrame sources) {
        Index newColumns = sources.getColumnsIndex().addLabels(CreateSegment.TARGET_COLUMN, CreateSegment.TARGET_CREATED_COLUMN);
        return sources.map(newColumns, (from, to) -> mapRow(from, to, cayenneContext));
    }

    protected void mapRow(RowProxy from, RowBuilder to, ObjectContext cayenneContext) {
        from.copy(to);

        to.set(CreateSegment.TARGET_COLUMN, create(cayenneContext));
        to.set(CreateSegment.TARGET_CREATED_COLUMN, true);
    }

    protected Object create(ObjectContext context) {

        // Note that "context.newObject" is an impure function. Though we don't see its undesired side effects on
        // multiple iterations due to DataFrame "materialized" feature that transparently caches the results..

        return context.newObject(type);
    }
}
