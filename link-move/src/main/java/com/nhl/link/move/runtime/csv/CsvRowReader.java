package com.nhl.link.move.runtime.csv;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.Row;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;

import java.util.Iterator;
import java.util.List;

/**
 * @since 1.4
 */
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

    /**
     * @param delimiter Valid Unicode code point in hexadecimal form (e.g. 0x9 for horizontal tab) or a character literal.
     */
    public void setDelimiter(String delimiter) {
        if (delimiter.startsWith("0x")) {
            // we have a character code here
            int codePoint = Integer.valueOf(delimiter.substring(2, delimiter.length()), 16);
            if (Character.isValidCodePoint(codePoint) && Character.charCount(codePoint) == 1) {
                this.delimiter = (char) codePoint;
            } else {
                throw new LmRuntimeException("Invalid delimiter (not a valid Unicode code point): "
                        + delimiter);
            }
        } else if (delimiter.length() != 1) {
            throw new LmRuntimeException("Invalid delimiter (should be exactly one character): " + delimiter);
        }
        this.delimiter = delimiter.charAt(0);
    }

    public void setReadFrom(Integer readFrom) {
        if (readFrom <= 0) {
            throw new LmRuntimeException("Invalid line number: " + readFrom);
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
