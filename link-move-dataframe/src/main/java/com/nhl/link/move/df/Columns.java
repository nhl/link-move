package com.nhl.link.move.df;

import java.util.HashMap;
import java.util.Map;

public class Columns {

    private Column<?>[] columns;
    private Map<String, Integer> columnIndex;

    public Columns(Column<?>... columns) {
        this.columns = columns;
    }

    public Column<?>[] getColumns() {
        return columns;
    }

    public int size() {
        return columns.length;
    }

    public int position(Column<?> column) {
        return position(column.getName());
    }

    public int position(String columnName) {

        if (columnIndex == null) {
            Map<String, Integer> index = new HashMap<>();

            for (int i = 0; i < columns.length; i++) {
                Integer previous = index.put(columns[i].getName(), i);
                if (previous != null) {
                    throw new IllegalStateException("Duplicate column '" + columns[i].getName() +
                            "'. Found at " + previous + " and " + i);
                }
            }

            this.columnIndex = index;
        }

        return columnIndex.computeIfAbsent(columnName, n -> {
            throw new IllegalArgumentException("Column '" + n + "' is not present in Columns");
        });
    }

    public Columns rename(Map<String, String> oldToNewNames) {

        int len = size();

        Column<?>[] newColumns = new Column<?>[len];
        for (int i = 0; i < len; i++) {
            String newName = oldToNewNames.get(columns[i].getName());
            newColumns[i] = newName != null ? new Column<>(newName, columns[i].getType()) : columns[i];
        }

        return new Columns(newColumns);
    }
}
