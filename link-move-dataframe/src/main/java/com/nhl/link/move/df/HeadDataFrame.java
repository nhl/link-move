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
    public Iterator<DataRow> iterator() {

        return new Iterator<DataRow>() {

            private int counter = 0;
            private Iterator<DataRow> delegateIt = HeadDataFrame.this.delegate.iterator();
            private Index columns = HeadDataFrame.this.getColumns();

            @Override
            public boolean hasNext() {
                return counter < len && delegateIt.hasNext();
            }

            @Override
            public DataRow next() {
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
