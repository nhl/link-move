package com.nhl.link.etl.runtime.task.createorupdate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.nhl.link.etl.mapper.Mapper;

/**
 * @since 1.3
 */
public class SourceMapper {

	private Mapper mapper;

	public SourceMapper(Mapper mapper) {
		this.mapper = mapper;
	}

	public Map<Object, Map<String, Object>> map(List<Map<String, Object>> translatedSources) {
		Map<Object, Map<String, Object>> mappedSources = new HashMap<>();

		for (Map<String, Object> s : translatedSources) {
			// TODO: report dupes?
			mappedSources.put(mapper.keyForSource(s), s);
		}

		return mappedSources;
	}

}
