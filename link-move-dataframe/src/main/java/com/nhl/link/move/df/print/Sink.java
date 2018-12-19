package com.nhl.link.move.df.print;

@FunctionalInterface
public interface Sink extends AutoCloseable {

    Sink append(String string);

    default Sink appendln(String string) {
        return append(string).append(System.lineSeparator());
    }

    @Override
    default void close() {
        // do nothing ; remove exceptions from super signature
    }
}
