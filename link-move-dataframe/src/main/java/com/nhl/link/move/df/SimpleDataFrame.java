package com.nhl.link.move.df;

import com.nhl.link.move.df.print.InlinePrinter;

import java.util.Collections;
import java.util.Iterator;

public class SimpleDataFrame implements DataFrame {

    private Iterable<Object[]> source;
    private Index columns;

    public SimpleDataFrame(Index columns) {
        this(columns, Collections.emptyList());
    }

    protected SimpleDataFrame(Index columns, Iterable<Object[]> source) {
        this.source = source;
        this.columns = columns;
    }

    @Override
    public Index getColumns() {
        return columns;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return source.iterator();
    }

    @Override
    public String toString() {
        return InlinePrinter.getInstance().print(new StringBuilder("SimpleDataFrame ["), this).append("]").toString();
    }
}
