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
                switch (className) {
                    case "byte":
                        return Byte.TYPE;
                    case "int":
                        return Integer.TYPE;
                    case "short":
                        return Short.TYPE;
                    case "char":
                        return Character.TYPE;
                    case "double":
                        return Double.TYPE;
                    case "long":
                        return Long.TYPE;
                    case "float":
                        return Float.TYPE;
                    case "boolean":
                        return Boolean.TYPE;
                }

                throw e;
            }

            if (className.length() < 3) {
                throw new IllegalArgumentException("Invalid class name: " + className);
            }

            // TODO: support for multi-dim arrays
            className = className.substring(0, className.length() - 2);

            switch (className) {
                case "byte":
                    return byte[].class;
                case "int":
                    return int[].class;
                case "long":
                    return long[].class;
                case "short":
                    return short[].class;
                case "char":
                    return char[].class;
                case "double":
                    return double[].class;
                case "float":
                    return float[].class;
                case "boolean":
                    return boolean[].class;
                default:
                    // Object[]?
                    return Class.forName("[L" + className + ";");
            }
        }
    }
}
