package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.Row;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import org.apache.cayenne.DataRow;

import java.util.Iterator;
import java.util.Objects;

public class JdbcRowReader implements RowReader {

    private DataRowIterator rows;
    private RowAttribute[] rowHeader;

    public JdbcRowReader(RowAttribute[] rowHeader, DataRowIterator rows) {
        this.rows = Objects.requireNonNull(rows);
        this.rowHeader = Objects.requireNonNull(rowHeader);
    }

    @Override
    public Iterator<Row> iterator() {

        return new Iterator<Row>() {

            @Override
            public boolean hasNext() {
                return rows.hasNext();
            }

            @Override
            public Row next() {
                DataRow row = rows.next();
                return new DataRowRow(rowHeader, row);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void close() {
        rows.close();
    }
}
