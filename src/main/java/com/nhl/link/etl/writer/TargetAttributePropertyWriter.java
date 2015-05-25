package com.nhl.link.etl.writer;

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
		boolean updated = false;

		Object oldValue = property.readProperty(target);

		if (!Util.nullSafeEquals(oldValue, value)) {
			property.writeProperty(target, oldValue, value);
			updated = true;
		}

		return updated;
	}
}
