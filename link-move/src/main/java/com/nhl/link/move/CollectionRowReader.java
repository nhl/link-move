package com.nhl.link.move;

import java.util.Collection;
import java.util.Iterator;

/**
 * Simple generic and non-streaming {@link RowReader} that can be used by ETL
 * extractors that can't stream data from their backend.
 */
public class CollectionRowReader implements RowReader {

	private RowAttribute[] rowHeader;
	private Collection<Object[]> rows;

	public CollectionRowReader(RowAttribute[] rowHeader, Collection<Object[]> rows) {
		this.rowHeader = rowHeader;
		this.rows = rows;
	}

	@Override
	public void close() {
		// do nothing - the are no connections to handle..
	}

	@Override
	public RowAttribute[] getHeader() {
		return rowHeader;
	}

	@Override
	public Iterator<Object[]> iterator() {
		return rows.iterator();
	}

}
