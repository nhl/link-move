package com.nhl.link.etl.task.createorupdate;

import java.util.Map;

/**
 * @since 1.3
 */
public class CreateOrUpdateTuple<T> {

	private T target;
	private Map<String, Object> source;
	private boolean created;

	public CreateOrUpdateTuple(Map<String, Object> source, T target, boolean created) {
		this.target = target;
		this.source = source;
		this.created = created;
	}

	public T getTarget() {
		return target;
	}

	public Map<String, Object> getSource() {
		return source;
	}

	public boolean isCreated() {
		return created;
	}
}
