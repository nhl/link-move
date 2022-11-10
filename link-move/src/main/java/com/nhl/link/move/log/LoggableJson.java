package com.nhl.link.move.log;

/**
 * A utility class for fast simplified JSON generation used for logging. Don't try it to use for real JSON serialization.
 */
public class LoggableJson {

    public static void append(StringBuilder out, String key, Object val) {
        append(out, key, val, !(val instanceof Number));
    }

    public static void append(StringBuilder out, String key, Object val, boolean quote) {
        if (val == null) {
            return;
        }

        if (out.length() > 1) {
            out.append(',');
        }

        out.append("\"").append(key).append("\":");

        if (quote) {
            out.append("\"").append(val).append("\"");

        } else {
            out.append(val);
        }
    }
}
