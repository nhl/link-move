package com.nhl.link.move.runtime.task.create;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.Series;
import com.nhl.dflib.row.RowProxy;
import com.nhl.link.move.runtime.targetmodel.TargetAttribute;
import com.nhl.link.move.runtime.task.common.FkResolver;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;
import com.nhl.link.move.writer.TargetPropertyWriter;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

import java.util.Map;
import java.util.Set;

/**
 * @since 2.6
 */
public class CreateTargetMerger<T extends DataObject> {

    private FkResolver fkResolver;
    private TargetPropertyWriterFactory<T> writerFactory;

    public CreateTargetMerger(TargetPropertyWriterFactory<T> writerFactory, FkResolver fkResolver) {
        this.writerFactory = writerFactory;
        this.fkResolver = fkResolver;
    }

    public DataFrame merge(ObjectContext context, DataFrame df) {

        Map<TargetAttribute, Set<Object>> fks = fkResolver.collectFks(df, dataColumns(df));
        Map<TargetAttribute, Map<Object, Object>> related = fkResolver.fetchRelated(context, fks);

        return merge(fkResolver.resolveFks(df, related));
    }

    private DataFrame merge(DataFrame df) {

        Series<T> targets = df.getColumn(CreateSegment.TARGET_COLUMN);
        int len = targets.size();

        for (String label : dataColumns(df)) {

            TargetPropertyWriter writer = writerFactory.getOrCreateWriter(label);
            Series<?> values = df.getColumn(label);

            for (int i = 0; i < len; i++) {
                T target = targets.get(i);
                Object v = values.get(i);
                if (writer.willWrite(target, v)) {
                    writer.write(target, v);
                }
            }
        }

        return df;
    }

    private Index dataColumns(DataFrame df) {
        return df.getColumnsIndex().dropLabels(s -> s.startsWith("$lm_"));
    }
}
