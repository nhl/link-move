package com.nhl.link.move.runtime.task.create;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
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

        Index sourceSubIndex = df
                .getColumnsIndex()
                .dropLabels(CreateSegment.TARGET_COLUMN, CreateSegment.TARGET_CREATED_COLUMN);

        Map<TargetAttribute, Set<Object>> fks = fkResolver.collectFks(df, sourceSubIndex);
        Map<TargetAttribute, Map<Object, Object>> related = fkResolver.fetchRelated(context, fks);

        df.map(df.getColumnsIndex(), (f, t) -> fkResolver.resolveFks(f, t, related))
                .forEach(r -> merge(r, sourceSubIndex));
        return df;
    }

    private void merge(RowProxy r, Index sourceSubIndex) {

        T target = (T) r.get(CreateOrUpdateSegment.TARGET_COLUMN);

        for (String label : sourceSubIndex) {
            TargetPropertyWriter writer = writerFactory.getOrCreateWriter(label);

            Object val = r.get(sourceSubIndex.position(label));
            if (writer.willWrite(target, val)) {
                writer.write(target, val);
            }
        }
    }
}
