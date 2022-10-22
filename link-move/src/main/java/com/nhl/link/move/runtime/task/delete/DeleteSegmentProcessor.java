package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterMissingTargetsFiltered;
import com.nhl.link.move.annotation.AfterSourceKeysExtracted;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsExtracted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.runtime.task.StageListener;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

public class DeleteSegmentProcessor {

    private final TargetMapper targetMapper;
    private final ExtractSourceKeysStage sourceKeysExtractor;
    private final MissingTargetsFilterStage missingTargetsFilter;
    private final DeleteTargetStage deleter;
    private final Map<Class<? extends Annotation>, List<StageListener>> listeners;

    public DeleteSegmentProcessor(
            TargetMapper targetMapper,
            ExtractSourceKeysStage sourceKeysExtractor,
            MissingTargetsFilterStage missingTargetsFilter,
            DeleteTargetStage deleter,
            Map<Class<? extends Annotation>, List<StageListener>> listeners) {
        this.targetMapper = targetMapper;
        this.sourceKeysExtractor = sourceKeysExtractor;
        this.missingTargetsFilter = missingTargetsFilter;
        this.deleter = deleter;
        this.listeners = listeners;
    }

    public void process(Execution exec, DeleteSegment segment) {
        notifyListeners(AfterTargetsExtracted.class, exec, segment);

        mapTarget(exec, segment);
        extractSourceKeys(exec, segment);
        filterMissingTargets(exec, segment);
        deleteTarget(segment);
        commitTarget(exec, segment);
    }

    private void mapTarget(Execution exec, DeleteSegment segment) {
        segment.setMappedTargets(targetMapper.map(segment.getTargets()));
        notifyListeners(AfterTargetsMapped.class, exec, segment);
    }

    private void extractSourceKeys(Execution exec, DeleteSegment segment) {
        segment.setSourceKeys(sourceKeysExtractor.extractSourceKeys(exec, segment.getContext()));
        notifyListeners(AfterSourceKeysExtracted.class, exec, segment);
    }

    private void filterMissingTargets(Execution exec, DeleteSegment segment) {
        segment.setMissingTargets(missingTargetsFilter.filterMissing(
                segment.getMappedTargets(),
                segment.getSourceKeys())
        );
        notifyListeners(AfterMissingTargetsFiltered.class, exec, segment);
    }

    private void deleteTarget(DeleteSegment segment) {
        deleter.delete(segment.getContext(), segment.getMissingTargets());
    }

    private void commitTarget(Execution exec, DeleteSegment segment) {
        segment.getContext().commitChanges();
        notifyListeners(AfterTargetsCommitted.class, exec, segment);
    }

    private void notifyListeners(Class<? extends Annotation> type, Execution exec, DeleteSegment segment) {
        List<StageListener> listenersOfType = listeners.get(type);
        if (listenersOfType != null) {
            for (StageListener l : listenersOfType) {
                l.afterStageFinished(exec, segment);
            }
        }
    }
}
