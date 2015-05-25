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

	private final RowAttribute[] attributes;
	private DataRow row;

	DataRowRow(RowAttribute[] attributes, DataRow row) {
		this.attributes = attributes;
		this.row = row;
	}

	@Override
	public RowAttribute[] attributes() {
		return attributes;
	}

	@Override
	public Object get(RowAttribute attribute) {

		// nulls are valid, but missing keys are suspect, so add debugging for
		// this condition
		Object value = row.get(attribute.sourceName());
		if (value == null && !row.containsKey(attribute.sourceName())) {
			LOGGER.info("Key is missing in the source '" + attribute.sourceName() + "' ... ignoring");
		}

		return value;
	}
}
