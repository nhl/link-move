package com.nhl.link.framework.etl;

public interface Row {

	/**
	 * Reads a value of the current row corresponding to provided row attribute.
	 */
	Object get(RowAttribute attribute);

	/**
	 * Returns metadata describing row attributes.
	 */
	RowAttribute[] attributes();
}
