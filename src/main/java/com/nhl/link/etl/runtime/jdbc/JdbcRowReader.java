package com.nhl.link.etl.runtime.jdbc;

import java.util.Iterator;

import org.apache.cayenne.DataRow;
import org.apache.cayenne.ResultIterator;

import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowAttribute;
import com.nhl.link.etl.RowReader;

public class JdbcRowReader implements RowReader {

	private ResultIterator<DataRow> rows;
	private RowAttribute[] attributes;

	public JdbcRowReader(RowAttribute[] attributes, ResultIterator<DataRow> rows) {
		this.rows = rows;
		this.attributes = attributes;
	}

	@Override
	public Iterator<Row> iterator() {
		final Iterator<DataRow> drIt = rows.iterator();

		return new Iterator<Row>() {
			@Override
			public boolean hasNext() {
				return drIt.hasNext();
			}

			@Override
			public Row next() {
				return new DataRowRow(attributes, drIt.next());
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
