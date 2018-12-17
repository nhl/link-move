package com.nhl.link.move.df;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

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
    public Columns getColumns() {
        return delegate.getColumns();
    }

    @Override
    public int estimatedLength() {
        return len;
    }

    @Override
    public DataFrame renameColumns(Map<String, String> oldToNewNames) {
        return new HeadDataFrame(delegate.renameColumns(oldToNewNames), len);
    }

    @Override
    public <T> DataFrame convertType(String columnName, Class<T> targetType, Function<Object, T> typeConverter) {
        return new HeadDataFrame(delegate.convertType(columnName, targetType, typeConverter), len);
    }

    @Override
    public Iterator<DataRow> iterator() {

        return new Iterator<DataRow>() {

            private int counter = 0;
            private Iterator<DataRow> delegateIt = HeadDataFrame.this.delegate.iterator();

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
}
