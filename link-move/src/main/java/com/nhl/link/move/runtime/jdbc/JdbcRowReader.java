package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import org.apache.cayenne.DataRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;

public class JdbcRowReader implements RowReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRowReader.class);

    private DataRowIterator rows;
    private RowAttribute[] header;

    public JdbcRowReader(RowAttribute[] header, DataRowIterator rows) {
        this.rows = Objects.requireNonNull(rows);
        this.header = Objects.requireNonNull(header);
    }

    @Override
    public RowAttribute[] getHeader() {
        return header;
    }

    @Override
    public Iterator<Object[]> iterator() {

        return new Iterator<Object[]>() {

            @Override
            public boolean hasNext() {
                return rows.hasNext();
            }

            @Override
            public Object[] next() {
                return fromDataRow(rows.next());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    Object[] fromDataRow(DataRow dataRow) {

        Object[] row = new Object[header.length];

        for (int i = 0; i < header.length; i++) {

            String name = header[i].getSourceName();
            Object value = dataRow.get(name);

            // nulls are valid, but missing keys are suspect, so add debugging for this condition
            if (value == null && !dataRow.containsKey(name)) {
                LOGGER.info("Key is missing in the source '" + name + "' ... ignoring");
            }

            row[i] = value;
        }

        return row;
    }

    @Override
    public void close() {
        rows.close();
    }
}
