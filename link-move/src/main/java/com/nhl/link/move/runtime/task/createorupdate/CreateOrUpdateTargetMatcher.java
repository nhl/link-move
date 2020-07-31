package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.Series;
import com.nhl.link.move.mapper.Mapper;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.query.ObjectSelect;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @since 1.3
 */
public class CreateOrUpdateTargetMatcher<T> {

    private Class<T> type;
    private Mapper mapper;
    private Index index;

    public CreateOrUpdateTargetMatcher(Class<T> type, Mapper mapper) {
        this.type = type;
        this.mapper = mapper;
        this.index = Index.forLabels(CreateOrUpdateSegment.TARGET_COLUMN);
    }

    public DataFrame match(ObjectContext context, DataFrame df) {

        Map<Object, Expression> expressions = new LinkedHashMap<>();

        df.forEach(r -> expressions.computeIfAbsent(r.get(CreateOrUpdateSegment.KEY_COLUMN),
                key -> mapper.expressionForKey(key)));

        // no keys (?)
        if (expressions.isEmpty()) {
            return toDataFrame(Collections.emptyList());
        } else {
            return toDataFrame(ObjectSelect.query(type).where(ExpressionFactory.or(expressions.values())).select(context));
        }
    }

    private DataFrame toDataFrame(List<T> data) {
        return DataFrame.newFrame(index).columns(Series.forData(data));
    }
}
