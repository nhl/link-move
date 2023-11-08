package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.Series;
import com.nhl.link.move.mapper.Mapper;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.query.ObjectSelect;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @since 1.3
 */
public class CreateOrUpdateTargetMatcher {

    private final Class<?> type;
    private final String objEntityName;
    private final Mapper mapper;
    private final Index index;

    public CreateOrUpdateTargetMatcher(Class<?> type, Mapper mapper) {
        this(type, null, mapper);
    }

    public CreateOrUpdateTargetMatcher(String objEntityName, Mapper mapper) {
        this(null, objEntityName, mapper);
    }

    protected CreateOrUpdateTargetMatcher(Class<?> type, String objEntityName, Mapper mapper) {
        this.type = type;
        this.objEntityName = objEntityName;
        this.mapper = mapper;
        this.index = Index.forLabels(CreateOrUpdateSegment.TARGET_COLUMN);
    }

    public DataFrame match(ObjectContext context, DataFrame df) {

        Map<Object, Expression> expressions = new LinkedHashMap<>();

        df.forEach(r -> expressions.computeIfAbsent(r.get(CreateOrUpdateSegment.KEY_COLUMN), mapper::expressionForKey));

        // no keys (?)
        if (expressions.isEmpty()) {
            return DataFrame.empty(index);
        } else {
            List<?> objects =
                    (type != null ? ObjectSelect.query(type) : ObjectSelect.query(Object.class, objEntityName))
                    .where(ExpressionFactory.or(expressions.values()))
                    .select(context);
            return DataFrame.byColumn(index).of(Series.ofIterable(objects));
        }
    }
}
