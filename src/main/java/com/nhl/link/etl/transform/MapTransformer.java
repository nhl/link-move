package com.nhl.link.etl.transform;

import java.util.HashMap;
import java.util.Map;

import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowAttribute;
import com.nhl.link.etl.batch.BatchConverter;

/**
 * A simple direct transformer of the extracted data.
 * 
 * @since 1.1
 */
public class MapTransformer implements BatchConverter<Row, Map<String, Object>> {

	private static final BatchConverter<Row, Map<String, Object>> instance = new MapTransformer();

	public static BatchConverter<Row, Map<String, Object>> instance() {
		return instance;
	}

	private MapTransformer() {
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
