package com.nhl.link.move.runtime.task.createorupdate;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nhl.link.move.Execution;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterSourcesMapped;
import com.nhl.link.move.annotation.AfterTargetsMatched;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.load.LoadListener;
import com.nhl.link.move.runtime.task.StageListener;

/**
 * A stateless thread-safe processor for batch segments of a create-or-update
 * ETL task.
 * 
 * @since 1.3
 */
@SuppressWarnings("deprecation")
public class CreateOrUpdateSegmentProcessor<T extends DataObject> {

	private static final Logger LOGGER = LoggerFactory.getLogger(CreateOrUpdateSegmentProcessor.class);

	private RowConverter rowConverter;
	private SourceMapper mapper;
	private TargetMatcher<T> matcher;
	private CreateOrUpdateMerger<T> merger;

	@Deprecated
	private List<LoadListener<T>> loadListeners;

	private Map<Class<? extends Annotation>, List<StageListener>> listeners;

	public CreateOrUpdateSegmentProcessor(RowConverter rowConverter, SourceMapper mapper, TargetMatcher<T> matcher,
			CreateOrUpdateMerger<T> merger,
			Map<Class<? extends Annotation>, List<StageListener>> stageListeners,
			List<LoadListener<T>> loadListeners) {

		this.rowConverter = rowConverter;
		this.mapper = mapper;
		this.matcher = matcher;
		this.merger = merger;
		this.loadListeners = loadListeners;
		this.listeners = stageListeners;
	}

	public void process(Execution exec, CreateOrUpdateSegment<T> segment) {

		// execute create-or-update pipeline stages
		convertSrc(exec, segment);
		mapSrc(exec, segment);
		matchTarget(exec, segment);
		mergeToTarget(exec, segment);
		commitTarget(segment);
	}

	private void convertSrc(Execution exec, CreateOrUpdateSegment<T> segment) {
		segment.setSources(rowConverter.convert(segment.getSourceRows()));
		notifyListeners(AfterSourceRowsConverted.class, exec, segment);
	}

	private void mapSrc(Execution exec, CreateOrUpdateSegment<T> segment) {
		segment.setMappedSources(mapper.map(segment.getSources()));
		notifyListeners(AfterSourcesMapped.class, exec, segment);
	}

	private void matchTarget(Execution exec, CreateOrUpdateSegment<T> segment) {
		segment.setMatchedTargets(matcher.match(segment.getContext(), segment.getMappedSources()));
		notifyListeners(AfterTargetsMatched.class, exec, segment);
	}

	private void mergeToTarget(Execution exec, CreateOrUpdateSegment<T> segment) {
		segment.setMerged(merger.merge(segment.getContext(), segment.getMappedSources(), segment.getMatchedTargets()));
		notifyListeners(AfterTargetsMerged.class, exec, segment);
		callDeprecatedListeners(exec, segment);
	}

	@Deprecated
	private void callDeprecatedListeners(Execution exec, CreateOrUpdateSegment<T> segment) {
		if (!loadListeners.isEmpty()) {

			LOGGER.warn("*** Calling deprecated LoadListener's. "
					+ "Consider replacing them with annotated segment listeners. "
					+ "See 'com.nhl.link.etl.annotation' package ");

			for (CreateOrUpdateTuple<T> t : segment.getMerged()) {

				if (t.isCreated()) {
					for (LoadListener<T> l : loadListeners) {
						l.targetCreated(exec, t.getSource(), t.getTarget());
					}
				} else {
					for (LoadListener<T> l : loadListeners) {
						l.targetUpdated(exec, t.getSource(), t.getTarget());
					}
				}
			}
		}
	}

	private void commitTarget(CreateOrUpdateSegment<T> segment) {
		segment.getContext().commitChanges();

		// TODO: do we care for a listener here?
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
