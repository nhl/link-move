package com.nhl.link.move.runtime.task.sourcekeys;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.nhl.link.move.mapper.Mapper;

/**
 * @since 1.3
 */
public class SourceKeysCollector {
	private Mapper mapper;

	public SourceKeysCollector(Mapper mapper) {
		this.mapper = mapper;
	}

	public void collectSourceKeys(Set<Object> keys, List<Map<String, Object>> sources) {

		for (Map<String, Object> s : sources) {
			// TODO: report dupes?
			keys.add(mapper.keyForSource(s));
		}
	}
}
