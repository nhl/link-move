package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.common.CallbackExecutor;
import com.nhl.link.move.runtime.task.common.FkResolver;

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
    private final CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor;

    public CreateOrUpdateSegmentProcessor(
            RowConverter rowConverter,
            SourceMapper sourceMapper,
            CreateOrUpdateTargetMatcher matcher,
            CreateOrUpdateTargetMapper mapper,
            CreateOrUpdateTargetMerger merger,
            FkResolver fkResolver,
            CallbackExecutor<CreateOrUpdateStage, CreateOrUpdateSegment> callbackExecutor) {

        this.rowConverter = rowConverter;
        this.sourceMapper = sourceMapper;
        this.matcher = matcher;
        this.mapper = mapper;
        this.merger = merger;
        this.fkResolver = fkResolver;
        this.callbackExecutor = callbackExecutor;
    }

    public void process(Execution exec, CreateOrUpdateSegment segment) {

        callbackExecutor.executeCallbacks(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, exec, segment);

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
        callbackExecutor.executeCallbacks(CreateOrUpdateStage.CONVERT_SOURCE_ROWS, exec, segment);
    }

    private void mapSrc(Execution exec, CreateOrUpdateSegment segment) {
        segment.setMappedSources(sourceMapper.map(segment.getSources()));
        callbackExecutor.executeCallbacks(CreateOrUpdateStage.MAP_SOURCE, exec, segment);
    }

    private void matchTarget(Execution exec, CreateOrUpdateSegment segment) {
        segment.setMatchedTargets(matcher.match(segment.getContext(), segment.getMappedSources()));
        callbackExecutor.executeCallbacks(CreateOrUpdateStage.MATCH_TARGET, exec, segment);
    }

    private void mapToTarget(Execution exec, CreateOrUpdateSegment segment) {
        segment.setMapped(mapper.map(segment.getContext(), segment.getMappedSources(), segment.getMatchedTargets()));
        callbackExecutor.executeCallbacks(CreateOrUpdateStage.MAP_TARGET, exec, segment);
    }

    private void resolveFks(Execution exec, CreateOrUpdateSegment segment) {
        segment.setFksResolved(fkResolver.resolveFks(segment.getContext(), segment.getMapped()));
        callbackExecutor.executeCallbacks(CreateOrUpdateStage.RESOLVE_FK_VALUES, exec, segment);
    }

    private void mergeToTarget(Execution exec, CreateOrUpdateSegment segment) {
        segment.setMerged(merger.merge(segment.getFksResolved()));
        callbackExecutor.executeCallbacks(CreateOrUpdateStage.MERGE_TARGET, exec, segment);
    }

    private void commitTarget(Execution exec, CreateOrUpdateSegment segment) {
        segment.getContext().commitChanges();
        callbackExecutor.executeCallbacks(CreateOrUpdateStage.COMMIT_TARGET, exec, segment);
    }
}
