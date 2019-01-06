package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.df.DataFrame;
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
		return df.addColumn(CreateOrUpdateSegment.KEY_COLUMN, (c, r) -> mapper.keyForSource(c.getSourceIndex(), r));
	}
}
