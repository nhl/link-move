package com.nhl.link.move.df;

import java.util.function.Function;

public interface DataRow {

    default <T> T get(Column<T> column) {
        return (T) get(column.getName());
    }

    Object get(String columnName);

    Object get(int position);

    Columns getColumns();

    DataRow columns(Columns columns);

    <T> DataRow convertColumn(Columns newColumns, int position, Function<Object, T> typeConverter);

    default int size() {
        return getColumns().size();
    }
}
