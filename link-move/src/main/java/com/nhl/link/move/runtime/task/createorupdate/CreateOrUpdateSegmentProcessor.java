package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.*;
import com.nhl.link.move.runtime.task.StageListener;
import com.nhl.link.move.runtime.task.common.FkResolver;
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

    private final RowConverter rowConverter;
    private final SourceMapper sourceMapper;
    private final CreateOrUpdateTargetMatcher<T> matcher;
    private final CreateOrUpdateTargetMapper<T> mapper;
    private final CreateOrUpdateTargetMerger<T> merger;
    private final FkResolver fkResolver;


    private Map<Class<? extends Annotation>, List<StageListener>> listeners;

    public CreateOrUpdateSegmentProcessor(
            RowConverter rowConverter,
            SourceMapper sourceMapper,
            CreateOrUpdateTargetMatcher<T> matcher,
            CreateOrUpdateTargetMapper<T> mapper,
            CreateOrUpdateTargetMerger<T> merger,
            FkResolver fkResolver,
            Map<Class<? extends Annotation>, List<StageListener>> stageListeners) {

        this.rowConverter = rowConverter;
        this.sourceMapper = sourceMapper;
        this.matcher = matcher;
        this.mapper = mapper;
        this.merger = merger;
        this.fkResolver = fkResolver;
        this.listeners = stageListeners;
    }

    public void process(Execution exec, CreateOrUpdateSegment<T> segment) {

        notifyListeners(AfterSourceRowsExtracted.class, exec, segment);

        convertSrc(exec, segment);
        mapSrc(exec, segment);
        matchTarget(exec, segment);
        mapToTarget(exec, segment);
        resolveFks(exec, segment);
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

    private void resolveFks(Execution exec, CreateOrUpdateSegment<T> segment) {
        segment.setFksResolved(fkResolver.resolveFks(segment.getContext(), segment.getMapped()));
        notifyListeners(AfterFksResolved.class, exec, segment);
    }

    private void mergeToTarget(Execution exec, CreateOrUpdateSegment<T> segment) {
        segment.setMerged(merger.merge(segment.getFksResolved()));
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
