package com.nhl.link.etl.runtime.task.delete;

import org.apache.cayenne.DataObject;

import com.nhl.link.etl.Execution;

public class DeleteSegmentProcessor<T extends DataObject> {

	private TargetMapper<T> targetMapper;
	private SourceMatcher<T> sourceMatcher;
	private DeleteTargetStage<T> deleter;

	public DeleteSegmentProcessor(TargetMapper<T> targetMapper, SourceMatcher<T> sourceMatcher,
			DeleteTargetStage<T> deleter) {
		this.targetMapper = targetMapper;
		this.sourceMatcher = sourceMatcher;
		this.deleter = deleter;
	}

	public void process(Execution exec, DeleteSegment<T> segment) {

		// execute delete stages

		mapTarget(segment);
		matchSrc(segment);
		deleteTarget(segment);
		commitTarget(segment);
	}

	private void mapTarget(DeleteSegment<T> segment) {
		segment.setMappedTargets(targetMapper.map(segment.getTargets()));
	}

	private void matchSrc(DeleteSegment<T> segment) {
		segment.setMissingTargets(sourceMatcher.match(segment.getContext(), segment.getMappedTargets()));
	}

	private void deleteTarget(DeleteSegment<T> segment) {
		deleter.delete(segment.getContext(), segment.getMissingTargets());
	}

	private void commitTarget(DeleteSegment<T> segment) {
		segment.getContext().commitChanges();
	}
}
