package com.nhl.link.etl;

public class ArrayRow implements Row {

	private Object[] values;
	private RowAttribute[] attributes;

	public ArrayRow(RowAttribute[] attributes, Object[] values) {
		this.attributes = attributes;
		this.values = values;
	}

	@Override
	public RowAttribute[] attributes() {
		return attributes;
	}

	@Override
	public Object get(RowAttribute attribute) {
		return values[attribute.getOrdinal()];
	}
}
