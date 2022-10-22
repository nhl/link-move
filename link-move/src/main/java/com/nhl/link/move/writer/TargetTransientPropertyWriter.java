package com.nhl.link.move.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * @since 1.6
 */
public class TargetTransientPropertyWriter implements TargetPropertyWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TargetTransientPropertyWriter.class);

    private final Method setter;

    public TargetTransientPropertyWriter(Method setter) {
        this.setter = setter;
    }

    @Override
    public void write(Object target, Object value) {
        try {
            setter.invoke(target, value);
        } catch (Exception e) {
            LOGGER.warn("Failed to invoke setter method with name '" + setter.getName() +
                    "' on object of type: " + target.getClass().getName(), e);
        }
    }

    @Override
    public boolean willWrite(Object target, Object value) {
        return true;
    }
}
