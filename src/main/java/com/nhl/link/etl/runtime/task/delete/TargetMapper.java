package com.nhl.link.etl.runtime.task.delete;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.DataObject;

import com.nhl.link.etl.mapper.Mapper;

/**
 * @since 1.3
 */
public class TargetMapper<T extends DataObject> {

	private Mapper mapper;

	public TargetMapper(Mapper mapper) {
		this.mapper = mapper;
	}

	public Map<Object, T> map(List<T> targets) {

		Map<Object, T> mapped = new HashMap<>();

		for (T t : targets) {
			// TODO: report dupes?
			mapped.put(mapper.keyForTarget(t), t);
		}

		return mapped;
	}
}
