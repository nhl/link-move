package com.nhl.link.etl;

import java.util.Collection;
import java.util.Iterator;

/**
 * Simple generic and non-streaming {@link RowReader} that can be used by ETL
 * extractors that can't stream data from their backend.
 */
public class CollectionRowReader implements RowReader {

	private Collection<Row> rows;

	public CollectionRowReader(Collection<Row> rows) {
		this.rows = rows;
	}

	@Override
	public void close() {
		// do nothing - the are no connections to handle..
	}

	@Override
	public Iterator<Row> iterator() {
		return rows.iterator();
	}

}
