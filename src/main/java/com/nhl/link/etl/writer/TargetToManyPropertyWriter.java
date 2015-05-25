package com.nhl.link.etl.writer;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.reflect.ToManyProperty;

import com.nhl.link.etl.EtlRuntimeException;

/**
 * A {@link TargetPropertyWriter} for to-many relationships that simply throws
 * an exception.
 * 
 * @since 1.4
 */
public class TargetToManyPropertyWriter implements TargetPropertyWriter {

	private ToManyProperty property;

	public TargetToManyPropertyWriter(ToManyProperty property) {
		this.property = property;
	}

	@Override
	public boolean write(DataObject target, Object value) {
		throw new EtlRuntimeException("'" + property.getName()
				+ "' is a to-many relationship  and is not allowed to be synced from source.");
	}
}
