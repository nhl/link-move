package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Series;
import com.nhl.dflib.accumulator.BooleanAccumulator;
import com.nhl.link.move.runtime.task.common.ProcessorUtil;
import com.nhl.link.move.writer.TargetPropertyWriter;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;
import org.apache.cayenne.DataObject;

/**
 * @since 2.6
 */
public class CreateOrUpdateTargetMerger<T extends DataObject> {

    protected TargetPropertyWriterFactory writerFactory;

    public CreateOrUpdateTargetMerger(TargetPropertyWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    public DataFrame merge(DataFrame df) {

        Series<T> targets = df.getColumn(CreateOrUpdateSegment.TARGET_COLUMN);
        int len = targets.size();
        BooleanAccumulator changed = new BooleanAccumulator(len);

        Series<Boolean> created = df.getColumn(CreateOrUpdateSegment.TARGET_CREATED_COLUMN);
        created.forEach(changed::add);

        for (String label : ProcessorUtil.dataColumns(df)) {

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

        return df.selectRows(changed.toSeries());
    }
}
