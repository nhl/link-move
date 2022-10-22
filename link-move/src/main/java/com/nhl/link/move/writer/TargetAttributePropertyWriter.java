package com.nhl.link.move.writer;

import org.apache.cayenne.reflect.AttributeProperty;
import org.apache.cayenne.util.Util;

/**
 * @since 1.4
 */
public class TargetAttributePropertyWriter implements TargetPropertyWriter {

	private final AttributeProperty property;

	public TargetAttributePropertyWriter(AttributeProperty property) {
		this.property = property;
	}

	@Override
	public void write(Object target, Object value) {
		Object oldValue = property.readProperty(target);
		property.writeProperty(target, oldValue, value);
	}

	@Override
	public boolean willWrite(Object target, Object value) {
		Object oldValue = property.readProperty(target);
		return !Util.nullSafeEquals(oldValue, value);
	}
}
