package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterFksResolved;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.runtime.task.StageListener;
import com.nhl.link.move.runtime.task.common.FkResolver;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

/**
 * @since 2.6
 */
public class CreateSegmentProcessor {

    private final RowConverter rowConverter;
    private final Map<Class<? extends Annotation>, List<StageListener>> listeners;
    private final CreateTargetMapper mapper;
    private final CreateTargetMerger merger;
    private final FkResolver fkResolver;

    public CreateSegmentProcessor(
            RowConverter rowConverter,
            CreateTargetMapper mapper,
            CreateTargetMerger merger,
            FkResolver fkResolver,
            Map<Class<? extends Annotation>, List<StageListener>> listeners) {

        this.rowConverter = rowConverter;
        this.listeners = listeners;
        this.mapper = mapper;
        this.merger = merger;
        this.fkResolver = fkResolver;
    }

    public void process(Execution exec, CreateSegment segment) {
        notifyListeners(AfterSourceRowsExtracted.class, exec, segment);
        convertSrc(exec, segment);
        mapToTarget(exec, segment);
        resolveFks(exec, segment);
        mergeToTarget(exec, segment);
        commitTarget(exec, segment);
    }

    private void convertSrc(Execution exec, CreateSegment segment) {
        segment.setSources(rowConverter.convert(segment.getSourceRowsHeader(), segment.getSourceRows()));
        notifyListeners(AfterSourceRowsConverted.class, exec, segment);
    }

    private void mapToTarget(Execution exec, CreateSegment segment) {
        segment.setMapped(mapper.map(segment.getContext(), segment.getSources()));
        notifyListeners(AfterTargetsMapped.class, exec, segment);
    }

    private void resolveFks(Execution exec, CreateSegment segment) {
        segment.setFksResolved(fkResolver.resolveFks(segment.getContext(), segment.getMapped()));
        notifyListeners(AfterFksResolved.class, exec, segment);
    }

    private void mergeToTarget(Execution exec, CreateSegment segment) {
        segment.setMerged(merger.merge(segment.getFksResolved()));
        notifyListeners(AfterTargetsMerged.class, exec, segment);
    }

    private void commitTarget(Execution exec, CreateSegment segment) {
        segment.getContext().commitChanges();
        notifyListeners(AfterTargetsCommitted.class, exec, segment);
    }

    private void notifyListeners(Class<? extends Annotation> type, Execution exec, CreateSegment segment) {
        List<StageListener> listenersOfType = listeners.get(type);
        if (listenersOfType != null) {
            for (StageListener l : listenersOfType) {
                l.afterStageFinished(exec, segment);
            }
        }
    }
}
