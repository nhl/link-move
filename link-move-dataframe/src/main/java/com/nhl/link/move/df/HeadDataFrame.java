package com.nhl.link.move.df;

import com.nhl.link.move.df.print.InlinePrinter;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeadDataFrame implements DataFrame {

    private DataFrame source;
    private int len;

    public HeadDataFrame(DataFrame source, int len) {
        this.source = source;
        this.len = len;
    }

    @Override
    public DataFrame head(int len) {
        return len >= this.len ? this : source.head(len);
    }

    @Override
    public Index getColumns() {
        return source.getColumns();
    }

    @Override
    public long count() {

        // unlike other frames, using iterator in hope that it quits early ...
        long count = 0;
        Iterator<Object[]> it = source.iterator();
        while (it.hasNext()) {
            count++;
        }

        return count;
    }

    @Override
    public Iterator<Object[]> iterator() {

        return new Iterator<Object[]>() {

            private int counter = 0;
            private Iterator<Object[]> delegateIt = HeadDataFrame.this.source.iterator();

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
