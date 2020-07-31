package com.nhl.link.move.runtime.task.create;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.Series;
import com.nhl.dflib.row.RowProxy;
import com.nhl.link.move.runtime.targetmodel.TargetAttribute;
import com.nhl.link.move.runtime.task.common.FkResolver;
import com.nhl.link.move.runtime.task.common.TargetMerger;
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
public class CreateTargetMerger<T extends DataObject> extends TargetMerger<T> {

    public CreateTargetMerger(TargetPropertyWriterFactory<T> writerFactory, FkResolver fkResolver) {
        super(writerFactory, fkResolver);
    }

    @Override
    protected DataFrame merge(DataFrame df) {

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
}
