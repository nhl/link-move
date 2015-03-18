package com.nhl.link.etl.runtime.jdbc;

import org.apache.cayenne.DataRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowAttribute;

/**
 * An ETL row based on Cayenne DataRow structure.
 */
final class DataRowRow implements Row {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataRowRow.class);

	private final RowAttribute[] keys;
	private DataRow row;

	DataRowRow(RowAttribute[] keys, DataRow row) {
		this.keys = keys;
		this.row = row;
	}

	@Override
	public RowAttribute[] attributes() {
		return keys;
	}

	@Override
	public Object get(RowAttribute key) {

		// nulls are valid, but missing keys are suspect, so add debugging for
		// this condition
		Object value = row.get(key.sourceName());
		if (value == null && !row.containsKey(key.sourceName())) {
			LOGGER.info("Key is missing in the source '" + key.sourceName() + "' ... ignoring");
		}

		return value;
	}
}
