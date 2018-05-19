package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.Row;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.runtime.targetmodel.TargetAttribute;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import com.nhl.link.move.valueconverter.ValueConverterFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Re-maps a list of {@link Row} objects to a Map with keys in the target namespace.
 *
 * @since 1.3
 */
public class RowConverter {

    private TargetEntity targetEntity;
    private ValueConverterFactory converterFactory;

    public RowConverter(TargetEntity targetEntity, ValueConverterFactory converterFactory) {
        this.targetEntity = targetEntity;
        this.converterFactory = converterFactory;
    }

    public List<Map<String, Object>> convert(List<Row> rows) {

        List<Map<String, Object>> converted = new ArrayList<>(rows.size());

        for (Row r : rows) {
            converted.add(convert(r));
        }

        return converted;
    }

    private Map<String, Object> convert(Row source) {

        Map<String, Object> converted = new HashMap<>();

        for (RowAttribute key : source.attributes()) {
            Optional<TargetAttribute> attribute = targetEntity.getAttribute(key.getTargetPath());
            String path = attribute.isPresent() ? attribute.get().getNormalizedPath() : key.getTargetPath();
            Object value = attribute.isPresent() ? convertValue(attribute.get(), source.get(key)) : source.get(key);
            converted.put(path, value);
        }

        return converted;
    }

    private Object convertValue(TargetAttribute attribute, Object value) {
        return value != null
                ? converterFactory.getConverter(attribute.getType()).convert(value, attribute.getScale())
                : null;
    }
}
