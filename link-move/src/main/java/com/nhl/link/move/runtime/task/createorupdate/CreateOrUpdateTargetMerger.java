package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.dflib.BooleanSeries;
import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.Series;
import com.nhl.dflib.accumulator.BooleanAccumulator;
import com.nhl.dflib.row.RowBuilder;
import com.nhl.dflib.row.RowProxy;
import com.nhl.link.move.runtime.targetmodel.TargetAttribute;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import com.nhl.link.move.runtime.task.common.FkResolver;
import com.nhl.link.move.runtime.task.create.CreateSegment;
import com.nhl.link.move.writer.TargetPropertyWriter;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;
import org.apache.cayenne.Cayenne;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.Persistent;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.exp.parser.ASTDbPath;
import org.apache.cayenne.query.ObjectSelect;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @since 2.6
 */
public class CreateOrUpdateTargetMerger<T extends DataObject> {

    private FkResolver fkResolver;
    private TargetPropertyWriterFactory<T> writerFactory;

    public CreateOrUpdateTargetMerger(TargetPropertyWriterFactory<T> writerFactory, FkResolver fkResolver) {
        this.writerFactory = writerFactory;
        this.fkResolver = fkResolver;
    }

    public DataFrame merge(ObjectContext context, DataFrame df) {

        Map<TargetAttribute, Set<Object>> fks = fkResolver.collectFks(df, dataColumns(df));
        Map<TargetAttribute, Map<Object, Object>> related = fkResolver.fetchRelated(context, fks);

        return merge(fkResolver.resolveFks(df, related));
    }

    private DataFrame merge(DataFrame df) {

        Series<T> targets = df.getColumn(CreateOrUpdateSegment.TARGET_COLUMN);
        int len = targets.size();
        BooleanAccumulator changed = new BooleanAccumulator(len);

        Series<Boolean> created = df.getColumn(CreateOrUpdateSegment.TARGET_CREATED_COLUMN);
        created.forEach(changed::add);

        for (String label : dataColumns(df)) {

            TargetPropertyWriter writer = writerFactory.getOrCreateWriter(label);
            Series<?> values = df.getColumn(label);

            for (int i = 0; i < len; i++) {
                T target = targets.get(i);
                Object v = values.get(i);
                if (writer.willWrite(target, v)) {
                    changed.set(i, true);
                    writer.write(target, v);
                }
            }
        }

        return df.filterRows(changed.toSeries());
    }

    private Index dataColumns(DataFrame df) {
        return df.getColumnsIndex().dropLabels(s -> s.startsWith("$lm_"));
    }
}
