package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.annotation.AfterTargetsMerged;
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
    private CreateTargetMapper<T> mapper;
    private CreateTargetMerger<T> merger;


    public CreateSegmentProcessor(
            RowConverter rowConverter,
            CreateTargetMapper<T> mapper,
            CreateTargetMerger<T> merger,
            Map<Class<? extends Annotation>, List<StageListener>> listeners) {

        this.rowConverter = rowConverter;
        this.listeners = listeners;
        this.mapper = mapper;
        this.merger = merger;
    }

    public void process(Execution exec, CreateSegment<T> segment) {
        convertSrc(exec, segment);
        mapToTarget(exec, segment);
        mergeToTarget(exec, segment);
        commitTarget(exec, segment);
    }

    private void convertSrc(Execution exec, CreateSegment<T> segment) {
        segment.setSources(rowConverter.convert(segment.getSourceRowsHeader(), segment.getSourceRows()));
        notifyListeners(AfterSourceRowsConverted.class, exec, segment);
    }

    private void mapToTarget(Execution exec, CreateSegment<T> segment) {
        segment.setMapped(mapper.map(segment.getContext(), segment.getSources()));
        notifyListeners(AfterTargetsMapped.class, exec, segment);
    }

    private void mergeToTarget(Execution exec, CreateSegment<T> segment) {
        segment.setMerged(merger.merge(segment.getContext(), segment.getMapped()));
        notifyListeners(AfterTargetsMerged.class, exec, segment);
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
