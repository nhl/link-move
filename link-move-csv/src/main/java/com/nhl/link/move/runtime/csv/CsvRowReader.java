package com.nhl.link.move.runtime.csv;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.math.BigDecimal;
import java.util.Iterator;

public class CsvRowReader implements RowReader {

    private final RowAttribute[] header;
    private final CSVParser parser;

    private int readFrom;

    public CsvRowReader(RowAttribute[] header, CSVParser parser) {
        this.header = header;
        this.parser = parser;
    }

    private static Object getValue(Class<?> type, String value) {

        if (value == null) {
            return null;
        }

        String className = type.getName();
        switch (className) {
            case "java.lang.String": {
                return value;
            }
            case "java.lang.Boolean": {
                if (value.isEmpty()) {
                    return null;
                }
                switch (value) {
                    case "true":
                    case "1":
                    case "Y":
                    case "T": {
                        return true;
                    }
                    case "false":
                    case "0":
                    case "N":
                    case "F": {
                        return false;
                    }
                    default: {
                        throw new LmRuntimeException("Unknown boolean format: " + value);
                    }
                }
            }
            case "java.lang.Byte": {
                return value.isEmpty() ? Byte.valueOf((byte) 0) : Byte.valueOf(value);
            }
            case "java.lang.Short": {
                return value.isEmpty() ? Short.valueOf((short) 0) : Short.valueOf(value);
            }
            case "java.lang.Integer": {
                return value.isEmpty() ? Integer.valueOf(0) : Integer.valueOf(value);
            }
            case "java.lang.Long": {
                return value.isEmpty() ? Long.valueOf(0L) : Long.valueOf(value);
            }
            case "java.lang.Float": {
                return value.isEmpty() ? Float.valueOf(0f) : Float.valueOf(value);
            }
            case "java.lang.Double": {
                return value.isEmpty() ? Double.valueOf(0d) : Double.valueOf(value);
            }
            case "java.math.BigDecimal": {
                return value.isEmpty() ? BigDecimal.ZERO : BigDecimal.valueOf(Double.valueOf(value));
            }
            case "java.util.Date":
            case "java.sql.Date":
            case "java.sql.Time":
            case "java.sql.Timestamp": {
                // format ??
                return value.isEmpty() ? null : value;
            }
            default: {
                return value;
            }
        }
    }

    @Override
    public RowAttribute[] getHeader() {
        return header;
    }

    public void setReadFrom(Integer readFrom) {
        if (readFrom <= 0) {
            throw new LmRuntimeException("Invalid line number: " + readFrom);
        }
        this.readFrom = readFrom;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public Iterator<Object[]> iterator() {
        Iterator<CSVRecord> drIt = parser.iterator();

        // wind forward if needed
        int i = 0;
        while (++i < readFrom) {
            drIt.next();
        }

        return new Iterator<Object[]>() {
            @Override
            public boolean hasNext() {
                return drIt.hasNext();
            }

            @Override
            public Object[] next() {
                return fromCSV(drIt.next());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Object[] fromCSV(CSVRecord record) {

        Object[] row = new Object[header.length];

        for (int i = 0; i < header.length; i++) {
            row[i] = valueFromCSVRecord(header[i], record);
        }

        return row;
    }

    private Object valueFromCSVRecord(RowAttribute attribute, CSVRecord record) {
        return getValue(attribute.type(), record.get(attribute.getOrdinal()));
    }
}
