package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.dflib.DataFrame;
import com.nhl.link.move.mapper.Mapper;

import java.util.Set;

/**
 * @since 1.3
 */
public class SourceKeysCollector {
	private Mapper mapper;

	public SourceKeysCollector(Mapper mapper) {
		this.mapper = mapper;
	}

	public void collectSourceKeys(Set<Object> keys, DataFrame sources) {

		// TODO: report dupes?
		sources.forEach(r -> keys.add(mapper.keyForSource(r)));
	}
}
