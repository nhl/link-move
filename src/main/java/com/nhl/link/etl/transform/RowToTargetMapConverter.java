package com.nhl.link.etl.transform;

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
	public Map<String, Object> fromTemplate(Row source, Map<String, Object> targetTemplate) {

		// reusing template for new values
		targetTemplate.clear();

		for (RowAttribute key : source.attributes()) {
			targetTemplate.put(key.targetName(), source.get(key));
		}

		return targetTemplate;
	}
}
