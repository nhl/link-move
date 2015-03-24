package com.nhl.link.etl;

import java.util.Iterator;

import com.nhl.link.etl.stats.ExecutionStats;

/**
 * A {@link RowReader} decorator that counts each returned row using
 * {@link ExecutionStats}.
 * 
 * @since 1.3
 */
public class CountingRowReader implements RowReader {

	private RowReader delegate;
	private ExecutionStats stats;

	public CountingRowReader(RowReader delegate, ExecutionStats stats) {
		this.delegate = delegate;
		this.stats = stats;
	}

	public void close() {
		delegate.close();
	}

	public Iterator<Row> iterator() {
		final Iterator<Row> it = delegate.iterator();

		return new Iterator<Row>() {
			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public void remove() {
				it.remove();
			}

			@Override
			public Row next() {
				stats.incrementExtracted(1);
				return it.next();
			}
		};
	}

}
