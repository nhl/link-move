package com.nhl.link.move.writer;

import org.apache.cayenne.reflect.ToOneProperty;
import org.apache.cayenne.util.Util;

/**
 * @since 1.4
 */
public class TargetToOnePropertyWriter implements TargetPropertyWriter {

	private final ToOneProperty property;

	public TargetToOnePropertyWriter(ToOneProperty property) {
		this.property = property;
	}

	@Override
	public void write(Object target, Object value) {
		property.setTarget(target, value, true);
	}

	@Override
	public boolean willWrite(Object target, Object value) {
		Object oldValue = property.readProperty(target);
		return !Util.nullSafeEquals(oldValue, value);
	}
}
