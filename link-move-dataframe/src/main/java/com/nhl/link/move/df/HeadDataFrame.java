package com.nhl.link.move.df;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class HeadDataFrame implements DataFrame {

    private DataFrame delegate;
    private int len;
    private DataRowMapper rowMapper;

    public HeadDataFrame(DataFrame delegate, int len) {
        this(delegate, len, DataRowMapper.identity());
    }

    public HeadDataFrame(DataFrame delegate, int len, DataRowMapper rowMapper) {
        this.delegate = delegate;
        this.len = len;
        this.rowMapper = rowMapper;
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
    public int estimatedLength() {
        return len;
    }

    @Override
    public DataFrame renameColumns(Map<String, String> oldToNewNames) {
        return new HeadDataFrame(delegate.renameColumns(oldToNewNames), len);
    }

    @Override
    public DataFrame map(DataRowMapper m) {
        return new HeadDataFrame(delegate, len, rowMapper.andThen(m));
    }

    @Override
    public <T> DataFrame mapColumn(String columnName, ValueMapper<Object, T> typeConverter) {
        return new HeadDataFrame(delegate.mapColumn(columnName, typeConverter), len);
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
                return rowMapper.apply(delegateIt.next());
            }
        };
    }
}