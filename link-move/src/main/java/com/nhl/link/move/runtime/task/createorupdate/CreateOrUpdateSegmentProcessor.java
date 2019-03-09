package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterSourcesMapped;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.annotation.AfterTargetsMatched;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.runtime.task.StageListener;
import org.apache.cayenne.DataObject;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

/**
 * A stateless thread-safe processor for batch segments of a create-or-update
 * ETL task.
 *
 * @since 1.3
 */
public class CreateOrUpdateSegmentProcessor<T extends DataObject> {

    private RowConverter rowConverter;
    private SourceMapper sourceMapper;
    private TargetMatcher<T> matcher;
    private TargetMapper<T> mapper;
    private TargetMerger<T> merger;

    private Map<Class<? extends Annotation>, List<StageListener>> listeners;

    public CreateOrUpdateSegmentProcessor(
            RowConverter rowConverter,
            SourceMapper sourceMapper,
            TargetMatcher<T> matcher,
            TargetMapper<T> mapper,
            TargetMerger<T> merger,
            Map<Class<? extends Annotation>, List<StageListener>> stageListeners) {

        this.rowConverter = rowConverter;
        this.sourceMapper = sourceMapper;
        this.matcher = matcher;
        this.mapper = mapper;
        this.merger = merger;
        this.listeners = stageListeners;
    }

    public void process(Execution exec, CreateOrUpdateSegment<T> segment) {

        // execute create-or-update pipeline stages
        convertSrc(exec, segment);
        mapSrc(exec, segment);
        matchTarget(exec, segment);
        mapToTarget(exec, segment);
        mergeToTarget(exec, segment);
        commitTarget(exec, segment);
    }

    private void convertSrc(Execution exec, CreateOrUpdateSegment<T> segment) {
        segment.setSources(rowConverter.convert(segment.getSourceRowsHeader(), segment.getSourceRows()));
        notifyListeners(AfterSourceRowsConverted.class, exec, segment);
    }

    private void mapSrc(Execution exec, CreateOrUpdateSegment<T> segment) {
        segment.setMappedSources(sourceMapper.map(segment.getSources()));
        notifyListeners(AfterSourcesMapped.class, exec, segment);
    }

    private void matchTarget(Execution exec, CreateOrUpdateSegment<T> segment) {
        segment.setMatchedTargets(matcher.match(segment.getContext(), segment.getMappedSources()));
        notifyListeners(AfterTargetsMatched.class, exec, segment);
    }

    private void mapToTarget(Execution exec, CreateOrUpdateSegment<T> segment) {
        segment.setMapped(mapper.map(segment.getContext(), segment.getMappedSources(), segment.getMatchedTargets()));
        notifyListeners(AfterTargetsMapped.class, exec, segment);
    }

    private void mergeToTarget(Execution exec, CreateOrUpdateSegment<T> segment) {
        segment.setMerged(merger.merge(segment.getContext(), segment.getMapped()));
        notifyListeners(AfterTargetsMerged.class, exec, segment);
    }

    private void commitTarget(Execution exec, CreateOrUpdateSegment<T> segment) {
        segment.getContext().commitChanges();
        notifyListeners(AfterTargetsCommitted.class, exec, segment);
    }

    private void notifyListeners(Class<? extends Annotation> type, Execution exec, CreateOrUpdateSegment<T> segment) {
        List<StageListener> listenersOfType = listeners.get(type);
        if (listenersOfType != null) {
            for (StageListener l : listenersOfType) {
                l.afterStageFinished(exec, segment);
            }
        }
    }
}
