package com.nhl.link.move.runtime.task.create;

import org.dflib.DataFrame;
import org.dflib.Series;
import com.nhl.link.move.runtime.task.common.ProcessorUtil;
import com.nhl.link.move.writer.TargetPropertyWriter;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;

/**
 * @since 2.6
 */
public class CreateTargetMerger {

    protected final TargetPropertyWriterFactory writerFactory;

    public CreateTargetMerger(TargetPropertyWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    public DataFrame merge(DataFrame df) {

        Series<?> targets = df.getColumn(CreateSegment.TARGET_COLUMN);
        int len = targets.size();

        for (String label : ProcessorUtil.dataColumns(df)) {

            TargetPropertyWriter writer = writerFactory.getOrCreateWriter(label);
            Series<?> values = df.getColumn(label);

            for (int i = 0; i < len; i++) {
                Object target = targets.get(i);
                Object v = values.get(i);
                if (writer.willWrite(target, v)) {
                    writer.write(target, v);
                }
            }
        }

        return df;
    }
}
