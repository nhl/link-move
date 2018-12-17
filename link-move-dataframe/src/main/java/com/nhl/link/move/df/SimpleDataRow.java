package com.nhl.link.move.df;

import java.util.function.Function;

public class SimpleDataRow implements DataRow {

    private Columns columns;
    private Object[] values;

    public SimpleDataRow(Columns columns, Object... values) {
        this.columns = columns;
        this.values = values;
    }

    @Override
    public Object get(String columnName) {
        return values[columns.position(columnName)];
    }

    @Override
    public Object get(int position) {
        return values[position];
    }

    @Override
    public Columns getColumns() {
        return columns;
    }

    @Override
    public DataRow columns(Columns columns) {
        return new SimpleDataRow(columns, values);
    }

    @Override
    public <T> DataRow convertColumn(Columns newColumns, int position, Function<Object, T> typeConverter) {

        int width = values.length;

        Object[] newValues = new Object[width];
        System.arraycopy(values, 0, newValues, 0, width);
        newValues[position] = typeConverter.apply(values[position]);

        return new SimpleDataRow(newColumns, newValues);
    }
}
