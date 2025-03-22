package com.nhl.link.move.runtime.task.create;

import org.apache.cayenne.ObjectContext;
import org.dflib.DataFrame;

import static org.dflib.Exp.$val;

/**
 * @since 2.6
 */
public class CreateTargetMapper {

    private final Class<?> type;

    public CreateTargetMapper(Class<?> type) {
        this.type = type;
    }

    public DataFrame map(ObjectContext cayenneContext, DataFrame sources) {
        return sources
                .colsAppend(CreateSegment.TARGET_COLUMN, CreateSegment.TARGET_CREATED_COLUMN).merge(
                        $val(type).mapVal(t -> cayenneContext.newObject(t)),
                        $val(true)
                );
    }
}
