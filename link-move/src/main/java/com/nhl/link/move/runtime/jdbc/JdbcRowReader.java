package com.nhl.link.move.runtime.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.nhl.link.move.BaseRowAttribute;
import org.apache.cayenne.DataRow;
import org.apache.cayenne.ResultIterator;
import org.apache.cayenne.exp.parser.ASTDbPath;

import com.nhl.link.move.Row;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;

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

			// compiled dynamically unless explicitly set for RowReader...
			private RowAttribute[] itAttributes;

			{
				itAttributes = attributes;
			}

			// if not set, compiles attributes dynamically from a DataRow and
			// caches them in iterator
			private RowAttribute[] attributes(DataRow row) {

				if (itAttributes == null) {
					itAttributes = attributesFromDataRow(row);
				}

				return itAttributes;
			}

			@Override
			public boolean hasNext() {
				return drIt.hasNext();
			}

			@Override
			public Row next() {

				DataRow row = drIt.next();
				RowAttribute[] attributes = attributes(row);
				return new DataRowRow(attributes, row);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	RowAttribute[] attributesFromDataRow(DataRow row) {

		List<String> names = new ArrayList<>(row.keySet());

		// ensure predictable order on each run...
		Collections.sort(names);

		RowAttribute[] attributes = new RowAttribute[row.size()];
		for (int i = 0; i < attributes.length; i++) {
			String name = names.get(i);
			attributes[i] = new BaseRowAttribute(Object.class, name, ASTDbPath.DB_PREFIX + name, i);
		}

		return attributes;
	}

	@Override
	public void close() {
		rows.close();
	}
}
