package com.nhl.link.move.writer;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.reflect.AttributeProperty;
import org.apache.cayenne.util.Util;

/**
 * @since 1.4
 */
public class TargetAttributePropertyWriter implements TargetPropertyWriter {

	private AttributeProperty property;

	public TargetAttributePropertyWriter(AttributeProperty property) {
		this.property = property;
	}

	@Override
	public boolean write(DataObject target, Object value) {
		Object oldValue = property.readProperty(target);
		property.writeProperty(target, oldValue, value);
		return true;
	}

	@Override
	public boolean willWrite(DataObject target, Object value) {
		Object oldValue = property.readProperty(target);
		return !Util.nullSafeEquals(oldValue, value);
	}
}
