package com.nhl.link.move.mapper;

import com.nhl.dflib.Index;
import com.nhl.dflib.row.RowBuilder;
import com.nhl.dflib.row.RowProxy;

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
}
