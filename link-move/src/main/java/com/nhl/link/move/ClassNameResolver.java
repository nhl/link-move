package com.nhl.link.move;

import java.util.Objects;

/**
 * @since 2.4
 */
public class ClassNameResolver {

    public static Class<?> typeForName(String className) {
        Objects.requireNonNull(className);

        try {
            return typeForNameWithExceptions(className);
        } catch (ClassNotFoundException e) {
            throw new LmRuntimeException("Class not found: " + className, e);
        }
    }

    private static Class<?> typeForNameWithExceptions(String className) throws ClassNotFoundException {

        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {

            // copied from Cayenne DefaultAdhocObjectFactory, except for inner
            // classes and VOID that we will not support (yet?)

            if (!className.endsWith("[]")) {
                if ("byte".equals(className)) {
                    return Byte.TYPE;
                } else if ("int".equals(className)) {
                    return Integer.TYPE;
                } else if ("short".equals(className)) {
                    return Short.TYPE;
                } else if ("char".equals(className)) {
                    return Character.TYPE;
                } else if ("double".equals(className)) {
                    return Double.TYPE;
                } else if ("long".equals(className)) {
                    return Long.TYPE;
                } else if ("float".equals(className)) {
                    return Float.TYPE;
                } else if ("boolean".equals(className)) {
                    return Boolean.TYPE;
                }

                throw e;
            }

            if (className.length() < 3) {
                throw new IllegalArgumentException("Invalid class name: " + className);
            }

            // TODO: support for multi-dim arrays
            className = className.substring(0, className.length() - 2);

            if ("byte".equals(className)) {
                return byte[].class;
            } else if ("int".equals(className)) {
                return int[].class;
            } else if ("long".equals(className)) {
                return long[].class;
            } else if ("short".equals(className)) {
                return short[].class;
            } else if ("char".equals(className)) {
                return char[].class;
            } else if ("double".equals(className)) {
                return double[].class;
            } else if ("float".equals(className)) {
                return float[].class;
            } else if ("boolean".equals(className)) {
                return boolean[].class;
            } else {
                // Object[]?
                return Class.forName("[L" + className + ";");
            }
        }
    }
}
