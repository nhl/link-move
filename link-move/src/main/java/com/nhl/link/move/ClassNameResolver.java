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
                return switch (className) {
                    case "byte" -> Byte.TYPE;
                    case "int" -> Integer.TYPE;
                    case "short" -> Short.TYPE;
                    case "char" -> Character.TYPE;
                    case "double" -> Double.TYPE;
                    case "long" -> Long.TYPE;
                    case "float" -> Float.TYPE;
                    case "boolean" -> Boolean.TYPE;
                    default -> throw e;
                };
            }

            if (className.length() < 3) {
                throw new IllegalArgumentException("Invalid class name: " + className);
            }

            // TODO: support for multi-dim arrays
            className = className.substring(0, className.length() - 2);

            return switch (className) {
                case "byte" -> byte[].class;
                case "int" -> int[].class;
                case "long" -> long[].class;
                case "short" -> short[].class;
                case "char" -> char[].class;
                case "double" -> double[].class;
                case "float" -> float[].class;
                case "boolean" -> boolean[].class;
                // Object[]?
                default -> Class.forName("[L" + className + ";");
            };
        }
    }
}
