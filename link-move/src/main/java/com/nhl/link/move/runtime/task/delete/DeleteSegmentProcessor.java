package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.common.CallbackExecutor;

public class DeleteSegmentProcessor {

    private final TargetMapper targetMapper;
    private final MissingTargetsFilterStage missingTargetsFilter;
    private final DeleteTargetStage deleter;
    private final CallbackExecutor<DeleteStage, DeleteSegment> callbackExecutor;

    public DeleteSegmentProcessor(
            TargetMapper targetMapper,
            MissingTargetsFilterStage missingTargetsFilter,
            DeleteTargetStage deleter,
            CallbackExecutor<DeleteStage, DeleteSegment> callbackExecutor) {
        this.targetMapper = targetMapper;
        this.missingTargetsFilter = missingTargetsFilter;
        this.deleter = deleter;
        this.callbackExecutor = callbackExecutor;
    }

    public void process(Execution exec, DeleteSegment segment) {
        callbackExecutor.executeCallbacks(DeleteStage.EXTRACT_TARGET, exec, segment);

        mapTarget(exec, segment);
        filterMissingTargets(exec, segment);
        deleteTarget(exec, segment);
        commitTarget(exec, segment);
    }

    private void mapTarget(Execution exec, DeleteSegment segment) {
        segment.setMappedTargets(targetMapper.map(segment.getTargets()));
        callbackExecutor.executeCallbacks(DeleteStage.MAP_TARGET, exec, segment);
    }

    private void filterMissingTargets(Execution exec, DeleteSegment segment) {
        segment.setMissingTargets(missingTargetsFilter.filterMissing(
                segment.getMappedTargets(),
                segment.getSourceKeys())
        );
        callbackExecutor.executeCallbacks(DeleteStage.FILTER_MISSING_TARGETS, exec, segment);
    }

    private void deleteTarget(Execution exec, DeleteSegment segment) {
        deleter.delete(segment.getContext(), segment.getMissingTargets());

        // copy without a change. This will help any callbacks to locate data for a given stage
        segment.setDeletedTargets(segment.getMissingTargets());

        callbackExecutor.executeCallbacks(DeleteStage.DELETE_TARGET, exec, segment);
    }

    private void commitTarget(Execution exec, DeleteSegment segment) {
        segment.getContext().commitChanges();
        callbackExecutor.executeCallbacks(DeleteStage.COMMIT_TARGET, exec, segment);
    }
}
