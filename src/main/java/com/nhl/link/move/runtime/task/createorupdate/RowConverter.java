package com.nhl.link.move.runtime.task.createorupdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.nhl.link.move.Row;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.runtime.path.EntityPathNormalizer;

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
			Object normalizedValue = pathNormalizer.normalizeValue(key.getTargetPath(), source.get(key));
			translated.put(path, normalizedValue);
		}

		return translated;
	}

	

}
