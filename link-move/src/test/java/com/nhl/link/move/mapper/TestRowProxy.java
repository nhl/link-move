package com.nhl.link.move.mapper;

import org.dflib.Index;
import org.dflib.row.RowBuilder;
import org.dflib.row.RowProxy;

public class TestRowProxy implements RowProxy {

    private Index index;
    private Object[] values;

    public TestRowProxy(Index index, Object... values) {
        this.index = index;
        this.values = values;
    }

    @Override
    public Index getIndex() {
        return index;
    }

    @Override
    public Object get(int columnPos) {
        return values[columnPos];
    }

    @Override
    public Object get(String columnName) {
        return get(index.position(columnName));
    }

    @Override
    public void copyRange(RowBuilder to, int fromOffset, int toOffset, int len) {
        throw new UnsupportedOperationException("not needed in a test");
    }

    @Override
    public int getInt(int i) {
        throw new UnsupportedOperationException("not needed in a test");
    }

    @Override
    public int getInt(String s) {
        throw new UnsupportedOperationException("not needed in a test");
    }

    @Override
    public long getLong(int i) {
        throw new UnsupportedOperationException("not needed in a test");
    }

    @Override
    public long getLong(String s) {
        throw new UnsupportedOperationException("not needed in a test");
    }

    @Override
    public double getDouble(int i) {
        throw new UnsupportedOperationException("not needed in a test");
    }

    @Override
    public double getDouble(String s) {
        throw new UnsupportedOperationException("not needed in a test");
    }

    @Override
    public boolean getBool(int i) {
        throw new UnsupportedOperationException("not needed in a test");
    }

    @Override
    public boolean getBool(String s) {
        throw new UnsupportedOperationException("not needed in a test");
    }
}
