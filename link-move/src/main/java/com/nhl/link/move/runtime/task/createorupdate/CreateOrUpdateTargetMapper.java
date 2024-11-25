package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Hasher;
import com.nhl.dflib.builder.BoolAccum;
import com.nhl.link.move.mapper.Mapper;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.access.DataContext;

/**
 * @since 2.6
 */
public class CreateOrUpdateTargetMapper {

    private final Class<?> type;
    private final String objEntityName;
    private final Mapper mapper;

    public CreateOrUpdateTargetMapper(Class<?> type, Mapper mapper) {
        this(type, null ,mapper);
    }

    public CreateOrUpdateTargetMapper(String objEntityName, Mapper mapper) {
        this(null, objEntityName, mapper);
    }

    protected CreateOrUpdateTargetMapper(Class<?> type, String objEntityName, Mapper mapper) {
        this.mapper = mapper;
        this.type = type;
        this.objEntityName = objEntityName;
    }

    public DataFrame map(
            ObjectContext context,
            DataFrame sources,
            DataFrame targets) {

        Hasher lkm = r -> r.get(CreateOrUpdateSegment.KEY_COLUMN);
        Hasher rkm = r -> mapper.keyForTarget(r.get(CreateOrUpdateSegment.TARGET_COLUMN));

        DataFrame df = sources.leftJoin().on(lkm, rkm).with(targets);

        BoolAccum createdColumn = new BoolAccum(df.height());
        df.forEach(r -> createdColumn.push(isCreated(r.get(CreateOrUpdateSegment.TARGET_COLUMN))));

        return df.addColumn(CreateOrUpdateSegment.TARGET_CREATED_COLUMN, createdColumn.toSeries())
                .convertColumn(CreateOrUpdateSegment.TARGET_COLUMN, r -> createIfMissing(r, context));
    }

    private boolean isCreated(Object v) {
        return v == null;
    }

    private Object createIfMissing(Object v, ObjectContext context) {

        // Note that "context.newObject" is an impure function. Though we don't see its undesired side effects on
        // multiple iterations due to DataFrame "materialized" feature that transparently caches the results..

        if (v != null) {
            return v;
        }
        return type != null ? context.newObject(type) : ((DataContext) context).newObject(objEntityName);
    }
}
