package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.runtime.task.StageListener;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;
import org.apache.cayenne.DataObject;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

/**
 * @param <T>
 * @since 2.6
 */
public class CreateSegmentProcessor<T extends DataObject> {

    private RowConverter rowConverter;
    private Map<Class<? extends Annotation>, List<StageListener>> listeners;
    private TargetCreator<T> targetCreator;

    public CreateSegmentProcessor(
            RowConverter rowConverter,
            TargetCreator<T> targetCreator,
            Map<Class<? extends Annotation>, List<StageListener>> listeners) {

        this.rowConverter = rowConverter;
        this.listeners = listeners;
        this.targetCreator = targetCreator;
    }

    public void process(Execution exec, CreateSegment<T> segment) {
        convertSrc(exec, segment);
        mapToTarget(exec, segment);
        commitTarget(exec, segment);
    }

    private void convertSrc(Execution exec, CreateSegment<T> segment) {
        segment.setSources(rowConverter.convert(segment.getSourceRows()));
        notifyListeners(AfterSourceRowsConverted.class, exec, segment);
    }

    private void mapToTarget(Execution exec, CreateSegment<T> segment) {

        // in case of "create" task, "mapping" means simply creating a new target object for each source map
        segment.setMerged(targetCreator.create(segment.getContext(), segment.getSources()));
        notifyListeners(AfterTargetsMapped.class, exec, segment);
    }

    private void commitTarget(Execution exec, CreateSegment<T> segment) {
        segment.getContext().commitChanges();
        notifyListeners(AfterTargetsCommitted.class, exec, segment);
    }

    private void notifyListeners(Class<? extends Annotation> type, Execution exec, CreateSegment<T> segment) {
        List<StageListener> listenersOfType = listeners.get(type);
        if (listenersOfType != null) {
            for (StageListener l : listenersOfType) {
                l.afterStageFinished(exec, segment);
            }
        }
    }
}
