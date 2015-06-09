package com.nhl.link.etl.runtime.task.createorupdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowAttribute;
import com.nhl.link.etl.runtime.path.EntityPathNormalizer;

/**
 * Re-maps a list of {@link Row} objects to a Map with keys in the target
 * namespace.
 * 
 * @since 1.3
 */
public class RowConverter {

	private EntityPathNormalizer pathNormalizer;

	public RowConverter(EntityPathNormalizer pathNormalizer) {
		this.pathNormalizer = pathNormalizer;
	}

	public List<Map<String, Object>> convert(List<Row> rows) {

		List<Map<String, Object>> translated = new ArrayList<>(rows.size());

		for (Row r : rows) {
			translated.add(convert(r));
		}

		return translated;
	}

	private Map<String, Object> convert(Row source) {

		// reusing template for new values
		Map<String, Object> translated = new HashMap<>();

		for (RowAttribute key : source.attributes()) {
			String path = pathNormalizer.normalize(key.getTargetPath());
			translated.put(path, source.get(key));
		}

		return translated;
	}

	

}
