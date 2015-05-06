package com.nhl.link.etl.runtime.file.csv;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowAttribute;

import java.io.IOException;

public class CsvDataRow implements Row {

    private RowAttribute[] attributes;
    private String data;
    private char delimiter;
    private String[] values;

    public CsvDataRow(RowAttribute[] attributes, String data, char delimiter) {
        this.attributes = attributes;
        this.data = data;
        this.delimiter = delimiter;
    }

    @Override
    public Object get(RowAttribute attribute) {
        if (values == null) {
            try {
                values = CsvUtils.parse(new String[attributes.length], data, delimiter);
            } catch (IOException e) {
                throw new EtlRuntimeException("Failed to parse CSV row", e);
            }
        }
        return values[attribute.ordinal()];
    }

    @Override
    public RowAttribute[] attributes() {
        return attributes;
    }

}
