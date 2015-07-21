package com.nhl.link.move.runtime.task.delete;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cayenne.ObjectContext;

public class DeleteSegment<T> {

	private ObjectContext context;
	private List<T> targets;

	private Set<Object> sourceKeys;
	private Map<Object, T> mappedTargets;
	private List<T> missingTargets;

	public DeleteSegment(ObjectContext context, List<T> targets) {
		this.targets = targets;
		this.context = context;
	}

	public ObjectContext getContext() {
		return context;
	}

	public List<T> getTargets() {
		return targets;
	}

	public Set<Object> getSourceKeys() {
		return sourceKeys;
	}

	public void setSourceKeys(Set<Object> sourceKeys) {
		this.sourceKeys = sourceKeys;
	}

	public Map<Object, T> getMappedTargets() {
		return mappedTargets;
	}

	public void setMappedTargets(Map<Object, T> mappedTargets) {
		this.mappedTargets = mappedTargets;
	}

	public List<T> getMissingTargets() {
		return missingTargets;
	}

	public void setMissingTargets(List<T> deleted) {
		this.missingTargets = deleted;
	}
}
