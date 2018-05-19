package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.runtime.task.SourceTargetPair;
import com.nhl.link.move.writer.TargetPropertyWriter;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;
import org.apache.cayenne.DataObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @since 2.6
 */
public class TargetMerger<T extends DataObject> {

    private TargetPropertyWriterFactory<T> writerFactory;

    public TargetMerger(TargetPropertyWriterFactory<T> writerFactory) {
        this.writerFactory = writerFactory;
    }

    public List<SourceTargetPair<T>> merge(List<SourceTargetPair<T>> mapped) {

        // merged list may be shorter as we are excluding phantom updates...
        List<SourceTargetPair<T>> merged = new ArrayList<>(mapped.size());
        for (SourceTargetPair<T> t : mapped) {
            if (merge(t)) {
                merged.add(t);
            }
        }

        return merged;
    }

    /**
     * @return true if the target has changed as a result of the merge.
     */
    private boolean merge(SourceTargetPair<T> pair) {

        boolean changed = pair.isCreated();

        if (pair.getSource().isEmpty()) {
            return changed;
        }

        T target = pair.getTarget();

        for (Map.Entry<String, Object> e : pair.getSource().entrySet()) {
            TargetPropertyWriter writer = writerFactory.getOrCreateWriter(e.getKey());

            if (writer.willWrite(target, e.getValue())) {
                changed = true;
                writer.write(target, e.getValue());
            }
        }

        return changed;
    }
}
