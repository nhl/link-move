package com.nhl.link.move.runtime.task.create;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Series;
import com.nhl.link.move.runtime.task.common.ProcessorUtil;
import com.nhl.link.move.writer.TargetPropertyWriter;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;
import org.apache.cayenne.DataObject;

/**
 * @since 2.6
 */
public class CreateTargetMerger<T extends DataObject> {

    protected TargetPropertyWriterFactory<T> writerFactory;

    public CreateTargetMerger(TargetPropertyWriterFactory<T> writerFactory) {
        this.writerFactory = writerFactory;
    }

    public DataFrame merge(DataFrame df) {

        Series<T> targets = df.getColumn(CreateSegment.TARGET_COLUMN);
        int len = targets.size();

        for (String label : ProcessorUtil.dataColumns(df)) {

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
