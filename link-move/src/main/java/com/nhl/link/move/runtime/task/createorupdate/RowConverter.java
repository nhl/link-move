package com.nhl.link.move.runtime.task.createorupdate;

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
 * Re-maps a list of source rows to a Map with keys in the target namespace.
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

    public List<Map<String, Object>> convert(RowAttribute[] rowHeader, List<Object[]> rows) {

        List<Map<String, Object>> converted = new ArrayList<>(rows.size());

        for (Object[] r : rows) {
            converted.add(convert(rowHeader, r));
        }

        return converted;
    }

    private Map<String, Object> convert(RowAttribute[] rowHeader, Object[] source) {

        Map<String, Object> converted = new HashMap<>();

        for (int i = 0; i < rowHeader.length; i++) {
            RowAttribute key = rowHeader[i];

            // TODO: should we "compile" this info for the header upfront and apply it to every row?
            Optional<TargetAttribute> attribute = targetEntity.getAttribute(key.getTargetPath());
            String path = attribute.isPresent() ? attribute.get().getNormalizedPath() : key.getTargetPath();
            Object value = attribute.isPresent() ? convertValue(attribute.get(), source[i]) : source[i];
            converted.put(path, value);
        }

        return converted;
    }

    private Object convertValue(TargetAttribute attribute, Object value) {
        return value != null
                ? converterFactory.getConverter(attribute.getJavaType()).convert(value, attribute.getScale())
                : null;
    }
}
