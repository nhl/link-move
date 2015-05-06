package com.nhl.link.etl.runtime.file.csv;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowAttribute;
import com.nhl.link.etl.RowReader;

import java.util.Iterator;
import java.util.List;

public class CsvRowReader implements RowReader {

    public static final char DEFAULT_DELIMITER = ',';

    private RowAttribute[] attributes;
    private List<String> data;
    private char delimiter;
    private Integer readFrom;

    public CsvRowReader(RowAttribute[] attributes, List<String> data) {
        this.attributes = attributes;
        this.data = data;
        this.delimiter = DEFAULT_DELIMITER;
        this.readFrom = 1;
    }

    public void setDelimiter(String delimiter) {
        if (delimiter.length() != 1) {
            throw new EtlRuntimeException("Invalid delimiter (should be exactly one character): " + delimiter);
        }
        this.delimiter = delimiter.charAt(0);
    }

    public void setReadFrom(Integer readFrom) {
        if (readFrom <= 0) {
            throw new EtlRuntimeException("Invalid line number: " + readFrom);
        }
        this.readFrom = readFrom;
    }

    @Override
    public Iterator<Row> iterator() {
        final Iterator<String> drIt = data.iterator();

        // wind forward if needed
        int i = 0;
        while (++i < readFrom) {
            drIt.next();
        }

        return new Iterator<Row>() {
            @Override
            public boolean hasNext() {
                return drIt.hasNext();
            }

            @Override
            public Row next() {
                return new CsvDataRow(attributes, drIt.next(), delimiter);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void close() {
        // do nothing
    }
}
