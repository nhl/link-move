package com.nhl.link.move.runtime.csv;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.Row;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.util.Iterator;

public class CsvRowReader implements RowReader {

	private final RowAttribute[] attributes;
	private final CSVParser parser;

	private int readFrom;

	public CsvRowReader(RowAttribute[] attributes, CSVParser parser) {
		this.attributes = attributes;
		this.parser = parser;
	}

	public void setReadFrom(Integer readFrom) {
		if (readFrom <= 0) {
			throw new LmRuntimeException("Invalid line number: " + readFrom);
		}
		this.readFrom = readFrom;
	}

	@Override
	public Iterator<Row> iterator() {
		Iterator<CSVRecord> drIt = parser.iterator();

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
				return new CsvDataRow(attributes, drIt.next());
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
