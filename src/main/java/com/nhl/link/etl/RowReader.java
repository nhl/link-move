package com.nhl.link.etl;

/**
 * An iterator over the source data of the ETL. Each data "row" is represented
 * as an Object[].
 */
public interface RowReader extends AutoCloseable, Iterable<Row> {

	/**
	 * Overrides super 'close' method to keep a no-exception signature.
	 */
	@Override
	void close();
}
