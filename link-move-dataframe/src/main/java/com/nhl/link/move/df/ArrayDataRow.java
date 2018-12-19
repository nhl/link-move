package com.nhl.link.move.df;

import com.nhl.link.move.df.print.InlinePrinter;

public class ArrayDataRow implements DataRow {

    private Index index;
    private Object[] values;

    public ArrayDataRow(Index index, Object... values) {
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
    public DataRow reindex(Index columns) {
        return new ArrayDataRow(columns, values);
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
