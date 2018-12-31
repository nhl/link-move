package com.nhl.link.move.df;

import com.nhl.link.move.df.filter.DataRowPredicate;
import com.nhl.link.move.df.print.InlinePrinter;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class FilteredDataFrame implements DataFrame {

    private Iterable<Object[]> source;
    private Index columns;
    private DataRowPredicate rowFilter;

    public FilteredDataFrame(Index columns, Iterable<Object[]> source, DataRowPredicate rowFilter) {
        this.source = source;
        this.columns = columns;
        this.rowFilter = rowFilter;
    }

    @Override
    public Index getColumns() {
        return columns;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new Iterator<Object[]>() {

            private Iterator<Object[]> delegateIt = FilteredDataFrame.this.source.iterator();
            private Object[] lastResolved;

            {
                rewind();
            }

            private void rewind() {
                lastResolved = null;
                while (delegateIt.hasNext()) {
                    Object[] next = delegateIt.next();
                    if (rowFilter.test(next)) {
                        lastResolved = next;
                        break;
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return lastResolved != null;
            }

            @Override
            public Object[] next() {

                if (lastResolved == null) {
                    throw new NoSuchElementException("No next element");
                }

                Object[] next = lastResolved;
                rewind();
                return next;
            }
        };
    }

    @Override
    public String toString() {
        return InlinePrinter.getInstance().print(new StringBuilder("FilteredDataFrame ["), this).append("]").toString();
    }
}
