package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.common.CallbackExecutor;
import com.nhl.link.move.runtime.task.common.FkResolver;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;

/**
 * @since 2.6
 */
public class CreateSegmentProcessor {

    private final RowConverter rowConverter;
    private final CallbackExecutor<CreateStage, CreateSegment> callbackExecutor;
    private final CreateTargetMapper mapper;
    private final CreateTargetMerger merger;
    private final FkResolver fkResolver;

    public CreateSegmentProcessor(
            RowConverter rowConverter,
            CreateTargetMapper mapper,
            CreateTargetMerger merger,
            FkResolver fkResolver,
            CallbackExecutor<CreateStage, CreateSegment> callbackExecutor) {

        this.rowConverter = rowConverter;
        this.callbackExecutor = callbackExecutor;
        this.mapper = mapper;
        this.merger = merger;
        this.fkResolver = fkResolver;
    }

    public void process(Execution exec, CreateSegment segment) {
        callbackExecutor.executeCallbacks(CreateStage.EXTRACT_SOURCE_ROWS, exec, segment);
        convertSrc(exec, segment);
        mapToTarget(exec, segment);
        resolveFks(exec, segment);
        mergeToTarget(exec, segment);
        commitTarget(exec, segment);
    }

    private void convertSrc(Execution exec, CreateSegment segment) {
        segment.setSources(rowConverter.convert(segment.getSourceRowsHeader(), segment.getSourceRows()));
        callbackExecutor.executeCallbacks(CreateStage.CONVERT_SOURCE_ROWS, exec, segment);
    }

    private void mapToTarget(Execution exec, CreateSegment segment) {
        segment.setMapped(mapper.map(segment.getContext(), segment.getSources()));
        callbackExecutor.executeCallbacks(CreateStage.MAP_TARGET, exec, segment);
    }

    private void resolveFks(Execution exec, CreateSegment segment) {
        segment.setFksResolved(fkResolver.resolveFks(segment.getContext(), segment.getMapped()));
        callbackExecutor.executeCallbacks(CreateStage.RESOLVE_FK_VALUES, exec, segment);
    }

    private void mergeToTarget(Execution exec, CreateSegment segment) {
        segment.setMerged(merger.merge(segment.getFksResolved()));
        callbackExecutor.executeCallbacks(CreateStage.MERGE_TARGET, exec, segment);
    }

    private void commitTarget(Execution exec, CreateSegment segment) {
        segment.getContext().commitChanges();
        callbackExecutor.executeCallbacks(CreateStage.COMMIT_TARGET, exec, segment);
    }
}
