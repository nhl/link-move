package com.nhl.link.move.runtime.task.sourcekeys;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.nhl.dflib.DataFrame;
import com.nhl.link.move.mapper.Mapper;

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
		sources.consume((c, r) -> keys.add(mapper.keyForSource(c, r)));
	}
}
