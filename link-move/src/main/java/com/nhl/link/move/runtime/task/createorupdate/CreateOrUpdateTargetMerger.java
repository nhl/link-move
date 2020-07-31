package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.row.RowBuilder;
import com.nhl.dflib.row.RowProxy;
import com.nhl.link.move.runtime.targetmodel.TargetAttribute;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import com.nhl.link.move.runtime.task.common.FkResolver;
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

        Index sourceSubIndex = df
                .getColumnsIndex()
                .dropLabels(CreateOrUpdateSegment.TARGET_COLUMN, CreateOrUpdateSegment.TARGET_CREATED_COLUMN);

        Map<TargetAttribute, Set<Object>> fks = fkResolver.collectFks(df, sourceSubIndex);
        Map<TargetAttribute, Map<Object, Object>> related = fkResolver.fetchRelated(context, fks);

        Index changeTrackingIndex = df.getColumnsIndex().addLabels(CreateOrUpdateSegment.TARGET_CHANGED_COLUMN);

        return df
                .map(df.getColumnsIndex(), (f, t) -> fkResolver.resolveFks(f, t, related))
                // TODO: rebuild "$lm_target" column individually instead of rebuilding the entire DataFrame
                .map(changeTrackingIndex, (f, t) -> merge(f, t, sourceSubIndex))
                .filterRows(r -> (boolean) r.get(CreateOrUpdateSegment.TARGET_CHANGED_COLUMN))
                .dropColumns(CreateOrUpdateSegment.TARGET_CHANGED_COLUMN);
    }


    private void merge(RowProxy from, RowBuilder to, Index sourceSubIndex) {

        boolean changed = (boolean) from.get(CreateOrUpdateSegment.TARGET_CREATED_COLUMN);
        T target = (T) from.get(CreateOrUpdateSegment.TARGET_COLUMN);

        from.copy(to);

        for (String label : sourceSubIndex) {
            TargetPropertyWriter writer = writerFactory.getOrCreateWriter(label);

            Object val = from.get(sourceSubIndex.position(label));
            if (writer.willWrite(target, val)) {
                changed = true;
                writer.write(target, val);
            }
        }

        to.set(CreateOrUpdateSegment.TARGET_CHANGED_COLUMN, changed);
    }
}
