package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.runtime.task.SourceTargetPair;
import com.nhl.link.move.writer.TargetPropertyWriter;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;
import org.apache.cayenne.DataObject;

import java.util.List;
import java.util.Map;

/**
 * @since 2.6
 */
public class CreateTargetMerger<T extends DataObject> {

    private TargetPropertyWriterFactory<T> writerFactory;

    public CreateTargetMerger(TargetPropertyWriterFactory<T> writerFactory) {
        this.writerFactory = writerFactory;
    }

    public List<SourceTargetPair<T>> merge(List<SourceTargetPair<T>> mapped) {
        mapped.forEach(this::merge);
        return mapped;
    }

    /**
     * @return true if the target has changed as a result of the merge.
     */
    private void merge(SourceTargetPair<T> pair) {

        T target = pair.getTarget();

        for (Map.Entry<String, Object> e : pair.getSource().entrySet()) {
            TargetPropertyWriter writer = writerFactory.getOrCreateWriter(e.getKey());

            if (writer.willWrite(target, e.getValue())) {
                writer.write(target, e.getValue());
            }
        }

    }
}
