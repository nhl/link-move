package com.nhl.link.etl.runtime.task.delete;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.DataObject;

import com.nhl.link.etl.Execution;
import com.nhl.link.etl.annotation.AfterMissingTargetsFiltered;
import com.nhl.link.etl.annotation.AfterTargetsMapped;
import com.nhl.link.etl.runtime.task.StageListener;

public class DeleteSegmentProcessor<T extends DataObject> {

	private TargetMapper<T> targetMapper;
	private MissingTargetsFilterStage<T> missingTargetsFilter;
	private DeleteTargetStage<T> deleter;
	private Map<Class<? extends Annotation>, List<StageListener>> listeners;

	public DeleteSegmentProcessor(TargetMapper<T> targetMapper, MissingTargetsFilterStage<T> missingTargetsFilter,
			DeleteTargetStage<T> deleter, Map<Class<? extends Annotation>, List<StageListener>> listeners) {
		this.targetMapper = targetMapper;
		this.missingTargetsFilter = missingTargetsFilter;
		this.deleter = deleter;
		this.listeners = listeners;
	}

	public void process(Execution exec, DeleteSegment<T> segment) {

		// execute delete stages

		mapTarget(exec, segment);
		filterMissingTargets(exec, segment);
		deleteTarget(segment);
		commitTarget(segment);
	}

	private void mapTarget(Execution exec, DeleteSegment<T> segment) {
		segment.setMappedTargets(targetMapper.map(segment.getTargets()));
		notifyListeners(AfterTargetsMapped.class, exec, segment);
	}

	private void filterMissingTargets(Execution exec, DeleteSegment<T> segment) {
		segment.setMissingTargets(missingTargetsFilter.filterMissing(exec, segment.getContext(), segment.getMappedTargets()));
		notifyListeners(AfterMissingTargetsFiltered.class, exec, segment);
	}

	private void deleteTarget(DeleteSegment<T> segment) {
		deleter.delete(segment.getContext(), segment.getMissingTargets());
	}

	private void commitTarget(DeleteSegment<T> segment) {
		segment.getContext().commitChanges();
	}

	private void notifyListeners(Class<? extends Annotation> type, Execution exec, DeleteSegment<T> segment) {
		List<StageListener> listenersOfType = listeners.get(type);
		if (listenersOfType != null) {
			for (StageListener l : listenersOfType) {
				l.afterStageFinished(exec, segment);
			}
		}
	}
}
