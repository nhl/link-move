package com.nhl.link.move.writer;

import org.apache.cayenne.DataObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * @since 1.6
 */
public class TargetTransientPropertyWriter implements TargetPropertyWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TargetTransientPropertyWriter.class);

    private Method setter;

    public TargetTransientPropertyWriter(Method setter) {
        this.setter = setter;
    }

    @Override
    public boolean write(DataObject target, Object value) {

        try {
            setter.invoke(target, value);
            return true;

        } catch (Exception e) {
            LOGGER.warn("Failed to invoke setter method with name '" + setter.getName() +
                    "' on object of type: " + target.getClass().getName(), e);
        }

        return false;
    }

    @Override
    public boolean willWrite(DataObject target, Object value) {
        return true;
    }
}
