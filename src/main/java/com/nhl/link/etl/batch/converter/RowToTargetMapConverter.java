package com.nhl.link.etl.batch.converter;

import java.util.HashMap;
import java.util.Map;

import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowAttribute;
import com.nhl.link.etl.batch.BatchConverter;

/**
 * Converts source {@link Row} to a map containing row column values by ETL
 * target key.
 * 
 * @since 1.3
 */
public class RowToTargetMapConverter implements BatchConverter<Row, Map<String, Object>> {

	private static final BatchConverter<Row, Map<String, Object>> instance = new RowToTargetMapConverter();

	public static BatchConverter<Row, Map<String, Object>> instance() {
		return instance;
	}

	private RowToTargetMapConverter() {
		// private noop constructor
	}

	@Override
	public Map<String, Object> createTemplate() {
		return new HashMap<>();
	}

	@Override
	public Map<String, Object> fromTemplate(Row rawSource, Map<String, Object> template) {

		// reusing template for new values
		template.clear();

		for (RowAttribute key : rawSource.attributes()) {
			template.put(key.targetName(), rawSource.get(key));
		}

		return template;
	}
}
