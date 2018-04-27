package com.nhl.link.move.runtime.task.createorupdate;

import java.util.List;
import java.util.Map;

import com.nhl.link.move.runtime.task.SourceTargetTuple;
import org.apache.cayenne.ObjectContext;

import com.nhl.link.move.Row;

/**
 * @since 1.3
 */
public class CreateOrUpdateSegment<T> {

	private ObjectContext context;
	private List<Row> sourceRows;

	private List<Map<String, Object>> sources;
	private Map<Object, Map<String, Object>> mappedSources;
	private List<T> matchedTargets;
	private List<SourceTargetTuple<T>> merged;

	public CreateOrUpdateSegment(ObjectContext context, List<Row> rows) {
		this.sourceRows = rows;
		this.context = context;
	}

	public ObjectContext getContext() {
		return context;
	}

	public List<Row> getSourceRows() {
		return sourceRows;
	}

	public List<Map<String, Object>> getSources() {
		return sources;
	}

	public void setSources(List<Map<String, Object>> translatedSegment) {
		this.sources = translatedSegment;
	}

	public Map<Object, Map<String, Object>> getMappedSources() {
		return mappedSources;
	}

	public void setMappedSources(Map<Object, Map<String, Object>> mappedSegment) {
		this.mappedSources = mappedSegment;
	}

	public List<T> getMatchedTargets() {
		return matchedTargets;
	}

	public void setMatchedTargets(List<T> matchedTargets) {
		this.matchedTargets = matchedTargets;
	}

	public List<SourceTargetTuple<T>> getMerged() {
		return merged;
	}

	public void setMerged(List<SourceTargetTuple<T>> merged) {
		this.merged = merged;
	}

}
