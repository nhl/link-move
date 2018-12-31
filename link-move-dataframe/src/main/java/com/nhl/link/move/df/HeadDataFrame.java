package com.nhl.link.move.df;

import com.nhl.link.move.df.print.InlinePrinter;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeadDataFrame implements DataFrame {

    private DataFrame delegate;
    private int len;

    public HeadDataFrame(DataFrame delegate, int len) {
        this.delegate = delegate;
        this.len = len;
    }

    @Override
    public DataFrame head(int len) {
        return len >= this.len ? this : delegate.head(len);
    }

    @Override
    public Index getColumns() {
        return delegate.getColumns();
    }

    @Override
    public Iterator<Object[]> iterator() {

        return new Iterator<Object[]>() {

            private int counter = 0;
            private Iterator<Object[]> delegateIt = HeadDataFrame.this.delegate.iterator();

            @Override
            public boolean hasNext() {
                return counter < len && delegateIt.hasNext();
            }

            @Override
            public Object[] next() {
                if (counter >= len) {
                    throw new NoSuchElementException("Past the end of the iterator");
                }

                counter++;
                return delegateIt.next();
            }
        };
    }

    @Override
    public String toString() {
        return InlinePrinter.getInstance().print(new StringBuilder("HeadDataFrame ["), this).append("]").toString();
    }
}
