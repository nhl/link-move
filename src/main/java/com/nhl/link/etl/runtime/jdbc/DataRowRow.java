package com.nhl.link.etl.runtime.jdbc;

import org.apache.cayenne.DataRow;

import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowAttribute;

/**
 * A flywheight row based on Cayenne DataRow structure.
 */
final class DataRowRow implements Row {

	private final RowAttribute[] keys;
	private DataRow row;

	DataRowRow(RowAttribute[] keys) {
		this.keys = keys;
	}

	@Override
	public RowAttribute[] attributes() {
		return keys;
	}

	@Override
	public Object get(RowAttribute key) {
		return row.get(key.sourceName());
	}

	/**
	 * Reinitializes DataRow.
	 */
	void setRow(DataRow row) {
		this.row = row;
	}
}
