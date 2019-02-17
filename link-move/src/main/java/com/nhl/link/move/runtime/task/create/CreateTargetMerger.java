package com.nhl.link.move.runtime.task.create;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.IndexPosition;
import com.nhl.dflib.row.RowProxy;
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

        df.forEach(r -> merge(r, sourceSubIndex));
        return df;
    }

    private void merge(RowProxy r, Index sourceSubIndex) {

        T target = (T) r.get(CreateOrUpdateSegment.TARGET_COLUMN);

        for (IndexPosition ip : sourceSubIndex) {
            TargetPropertyWriter writer = writerFactory.getOrCreateWriter(ip.name());

            Object val = r.get(ip.ordinal());
            if (writer.willWrite(target, val)) {
                writer.write(target, val);
            }
        }
    }
}
