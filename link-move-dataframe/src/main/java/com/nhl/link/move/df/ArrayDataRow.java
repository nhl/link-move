package com.nhl.link.move.df;

import com.nhl.link.move.df.map.ValueMapper;
import com.nhl.link.move.df.print.InlinePrinter;

public class ArrayDataRow implements DataRow {

    private Index index;
    private Object[] values;

    public ArrayDataRow(Index index, Object... values) {

        if (index.size() != values.length) {
            throw new IllegalArgumentException(String.format(
                    "Index size of %s is not the same as values size of %s",
                    index.size(),
                    values.length));
        }

        this.index = index;
        this.values = values;
    }

    @Override
    public Object get(String columnName) {
        return values[index.position(columnName)];
    }

    @Override
    public Object get(int position) {
        return values[position];
    }

    @Override
    public Index getIndex() {
        return index;
    }

    @Override
    public DataRow reindex(Index index) {
        return this.index == index ? this : new ArrayDataRow(index, values);
    }

    @Override
    public void copyTo(Object[] to, int toOffset) {
        System.arraycopy(values, 0, to, toOffset, values.length);
    }

    @Override
    public <V, VR> DataRow mapColumn(int position, ValueMapper<V, VR> m) {

        int width = values.length;

        Object[] newValues = new Object[width];
        System.arraycopy(values, 0, newValues, 0, width);
        newValues[position] = m.apply((V) values[position]);

        return new ArrayDataRow(index, newValues);
    }

    @Override
    public String toString() {
        return InlinePrinter.getInstance().print(new StringBuilder("ArrayDataRow "), this).toString();
    }
}
