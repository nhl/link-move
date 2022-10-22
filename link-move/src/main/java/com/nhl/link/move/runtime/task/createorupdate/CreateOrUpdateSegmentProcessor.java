package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterFksResolved;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;
import com.nhl.link.move.annotation.AfterSourcesMapped;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.annotation.AfterTargetsMatched;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.runtime.task.StageListener;
import com.nhl.link.move.runtime.task.common.FkResolver;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

/**
 * A stateless thread-safe processor for batch segments of a create-or-update ETL task.
 *
 * @since 1.3
 */
public class CreateOrUpdateSegmentProcessor {

    private final RowConverter rowConverter;
    private final SourceMapper sourceMapper;
    private final CreateOrUpdateTargetMatcher matcher;
    private final CreateOrUpdateTargetMapper mapper;
    private final CreateOrUpdateTargetMerger merger;
    private final FkResolver fkResolver;
    private final Map<Class<? extends Annotation>, List<StageListener>> listeners;

    public CreateOrUpdateSegmentProcessor(
            RowConverter rowConverter,
            SourceMapper sourceMapper,
            CreateOrUpdateTargetMatcher matcher,
            CreateOrUpdateTargetMapper mapper,
            CreateOrUpdateTargetMerger merger,
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

    public void process(Execution exec, CreateOrUpdateSegment segment) {

        notifyListeners(AfterSourceRowsExtracted.class, exec, segment);

        convertSrc(exec, segment);
        mapSrc(exec, segment);
        matchTarget(exec, segment);
        mapToTarget(exec, segment);
        resolveFks(exec, segment);
        mergeToTarget(exec, segment);
        commitTarget(exec, segment);
    }

    private void convertSrc(Execution exec, CreateOrUpdateSegment segment) {
        segment.setSources(rowConverter.convert(segment.getSourceRowsHeader(), segment.getSourceRows()));
        notifyListeners(AfterSourceRowsConverted.class, exec, segment);
    }

    private void mapSrc(Execution exec, CreateOrUpdateSegment segment) {
        segment.setMappedSources(sourceMapper.map(segment.getSources()));
        notifyListeners(AfterSourcesMapped.class, exec, segment);
    }

    private void matchTarget(Execution exec, CreateOrUpdateSegment segment) {
        segment.setMatchedTargets(matcher.match(segment.getContext(), segment.getMappedSources()));
        notifyListeners(AfterTargetsMatched.class, exec, segment);
    }

    private void mapToTarget(Execution exec, CreateOrUpdateSegment segment) {
        segment.setMapped(mapper.map(segment.getContext(), segment.getMappedSources(), segment.getMatchedTargets()));
        notifyListeners(AfterTargetsMapped.class, exec, segment);
    }

    private void resolveFks(Execution exec, CreateOrUpdateSegment segment) {
        segment.setFksResolved(fkResolver.resolveFks(segment.getContext(), segment.getMapped()));
        notifyListeners(AfterFksResolved.class, exec, segment);
    }

    private void mergeToTarget(Execution exec, CreateOrUpdateSegment segment) {
        segment.setMerged(merger.merge(segment.getFksResolved()));
        notifyListeners(AfterTargetsMerged.class, exec, segment);
    }

    private void commitTarget(Execution exec, CreateOrUpdateSegment segment) {
        segment.getContext().commitChanges();
        notifyListeners(AfterTargetsCommitted.class, exec, segment);
    }

    private void notifyListeners(Class<? extends Annotation> type, Execution exec, CreateOrUpdateSegment segment) {
        List<StageListener> listenersOfType = listeners.get(type);
        if (listenersOfType != null) {
            for (StageListener l : listenersOfType) {
                l.afterStageFinished(exec, segment);
            }
        }
    }
}
