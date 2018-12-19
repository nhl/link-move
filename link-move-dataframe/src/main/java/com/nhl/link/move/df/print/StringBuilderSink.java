package com.nhl.link.move.df.print;

public class StringBuilderSink implements Sink {

    // accumulate lines in the buffer, flushing every now and then
    private StringBuilder buffer;

    @Override
    public String toString() {
        return buffer != null ? buffer.toString() : "";
    }

    @Override
    public Sink append(String string) {
        createOrInitLineBuffer().append(string);
        return this;
    }

    private StringBuilder createOrInitLineBuffer() {
        return buffer != null ? buffer : (buffer = new StringBuilder());
    }
}
