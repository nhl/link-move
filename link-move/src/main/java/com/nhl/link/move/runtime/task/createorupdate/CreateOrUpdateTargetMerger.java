package com.nhl.link.move.runtime.task.createorupdate;

import org.dflib.BooleanSeries;
import org.dflib.DataFrame;
import org.dflib.Series;
import org.dflib.builder.BoolAccum;
import com.nhl.link.move.runtime.task.common.ProcessorUtil;
import com.nhl.link.move.writer.TargetPropertyWriter;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;

/**
 * @since 2.6
 */
public class CreateOrUpdateTargetMerger {

    protected final TargetPropertyWriterFactory writerFactory;

    public CreateOrUpdateTargetMerger(TargetPropertyWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    public DataFrame merge(DataFrame df) {

        Series<?> targets = df.getColumn(CreateOrUpdateSegment.TARGET_COLUMN);
        int len = targets.size();
        BoolAccum changed = new BoolAccum(len);

        BooleanSeries created = df.getColumn(CreateOrUpdateSegment.TARGET_CREATED_COLUMN).castAsBool();
        created.forEach(changed::push);

        for (String label : ProcessorUtil.dataColumns(df)) {

            TargetPropertyWriter writer = writerFactory.getOrCreateWriter(label);
            Series<?> values = df.getColumn(label);

            for (int i = 0; i < len; i++) {
                Object target = targets.get(i);
                Object v = values.get(i);
                if (writer.willWrite(target, v)) {
                    changed.replace(i, true);
                    writer.write(target, v);
                }
            }
        }

        return df.rows(changed.toSeries()).select();
    }
}
