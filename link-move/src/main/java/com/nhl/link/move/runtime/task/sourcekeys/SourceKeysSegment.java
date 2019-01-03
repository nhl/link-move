package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.link.move.RowAttribute;

import java.util.List;
import java.util.Map;

/**
 * @since 1.3
 */
public class SourceKeysSegment {

	private List<Object[]> sourceRows;
	private RowAttribute[] sourceRowsHeader;
	private List<Map<String, Object>> sources;

	public SourceKeysSegment(RowAttribute[] sourceRowsHeader, List<Object[]> sourceRows) {
		this.sourceRowsHeader = sourceRowsHeader;
		this.sourceRows = sourceRows;
	}

	public List<Object[]> getSourceRows() {
		return sourceRows;
	}

	public RowAttribute[] getSourceRowsHeader() {
		return sourceRowsHeader;
	}

	public List<Map<String, Object>> getSources() {
		return sources;
	}

	public void setSources(List<Map<String, Object>> sources) {
		this.sources = sources;
	}

}
