package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.df.DataFrame;

/**
 * @since 1.3
 */
public class SourceKeysSegment {

	private RowAttribute[] sourceRowsHeader;

	private DataFrame sourceRows;
	private DataFrame sources;

	public SourceKeysSegment(RowAttribute[] sourceRowsHeader, DataFrame sourceRows) {
		this.sourceRowsHeader = sourceRowsHeader;
		this.sourceRows = sourceRows;
	}

	public DataFrame getSourceRows() {
		return sourceRows;
	}

	public RowAttribute[] getSourceRowsHeader() {
		return sourceRowsHeader;
	}

	public DataFrame getSources() {
		return sources;
	}

	public void setSources(DataFrame sources) {
		this.sources = sources;
	}
}
