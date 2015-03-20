package com.nhl.link.etl.runtime.task.createorupdate;

import java.util.List;
import java.util.Map;

import org.apache.cayenne.ObjectContext;

import com.nhl.link.etl.Row;

/**
 * @since 1.3
 */
public class CreateOrUpdateSegment<T> {

	private ObjectContext context;
	private List<Row> rows;

	private List<Map<String, Object>> translatedSources;
	private Map<Object, Map<String, Object>> mappedSources;
	private List<T> matchedTargets;
	private List<CreateOrUpdateTuple<T>> merged;

	public CreateOrUpdateSegment(ObjectContext context, List<Row> rows) {
		this.rows = rows;
		this.context = context;
	}

	public ObjectContext getContext() {
		return context;
	}

	public List<Row> getRows() {
		return rows;
	}

	public List<Map<String, Object>> getTranslatedSources() {
		return translatedSources;
	}

	public void setTranslatedSources(List<Map<String, Object>> translatedSegment) {
		this.translatedSources = translatedSegment;
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

	public List<CreateOrUpdateTuple<T>> getMerged() {
		return merged;
	}

	public void setMerged(List<CreateOrUpdateTuple<T>> merged) {
		this.merged = merged;
	}

}
