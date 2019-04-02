package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.dflib.DataFrame;
import com.nhl.link.move.mapper.Mapper;

/**
 * @since 1.3
 */
public class SourceMapper {

	private Mapper mapper;

	public SourceMapper(Mapper mapper) {
		this.mapper = mapper;
	}

	public DataFrame map(DataFrame df) {
		return df.addColumn(CreateOrUpdateSegment.KEY_COLUMN, mapper::keyForSource);
	}
}
