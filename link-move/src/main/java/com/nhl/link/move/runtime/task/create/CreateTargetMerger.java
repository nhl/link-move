package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.df.DataFrame;
import com.nhl.link.move.df.Index;
import com.nhl.link.move.df.IndexPosition;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;
import com.nhl.link.move.writer.TargetPropertyWriter;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;
import org.apache.cayenne.DataObject;

/**
 * @since 2.6
 */
public class CreateTargetMerger<T extends DataObject> {

    private TargetPropertyWriterFactory<T> writerFactory;

    public CreateTargetMerger(TargetPropertyWriterFactory<T> writerFactory) {
        this.writerFactory = writerFactory;
    }

    public DataFrame merge(DataFrame df) {

        Index sourceSubIndex = df
                .getColumns()
                .dropNames(CreateSegment.TARGET_COLUMN, CreateSegment.TARGET_CREATED_COLUMN);

        df.consume((c, r) -> merge(c, r, sourceSubIndex));
        return df;
    }

    private void merge(Index columns, Object[] row, Index sourceSubIndex) {

        T target = (T) columns.get(row, CreateOrUpdateSegment.TARGET_COLUMN);

        for (IndexPosition ip : sourceSubIndex) {
            TargetPropertyWriter writer = writerFactory.getOrCreateWriter(ip.name());

            Object val = ip.get(row);
            if (writer.willWrite(target, val)) {
                writer.write(target, val);
            }
        }
    }
}
