package com.nhl.link.framework.etl.runtime.jdbc;

import java.util.Iterator;

import org.apache.cayenne.DataRow;
import org.apache.cayenne.ResultIterator;

import com.nhl.link.framework.etl.Row;
import com.nhl.link.framework.etl.RowReader;
import com.nhl.link.framework.etl.RowAttribute;

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
		final DataRowRow flyweightRow = new DataRowRow(attributes);

		return new Iterator<Row>() {
			@Override
			public boolean hasNext() {
				return drIt.hasNext();
			}

			@Override
			public Row next() {

				DataRow dr = drIt.next();
				flyweightRow.setRow(dr);
				return flyweightRow;
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
