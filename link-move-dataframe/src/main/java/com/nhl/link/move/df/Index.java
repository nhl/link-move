package com.nhl.link.move.df;

import java.util.HashMap;
import java.util.Map;

public class Index {

    private String[] columns;
    private Map<String, Integer> columnIndex;

    public Index(String... columns) {
        this.columns = columns;
    }

    public String[] getColumns() {
        return columns;
    }

    public int size() {
        return columns.length;
    }

    public int position(String columnName) {

        if (columnIndex == null) {
            Map<String, Integer> index = new HashMap<>();

            for (int i = 0; i < columns.length; i++) {
                Integer previous = index.put(columns[i], i);
                if (previous != null) {
                    throw new IllegalStateException("Duplicate column '" + columns[i] +
                            "'. Found at " + previous + " and " + i);
                }
            }

            this.columnIndex = index;
        }

        return columnIndex.computeIfAbsent(columnName, n -> {
            throw new IllegalArgumentException("Column '" + n + "' is not present in Columns");
        });
    }

    public Index rename(Map<String, String> oldToNewNames) {

        int len = size();

        String[] newColumns = new String[len];
        for (int i = 0; i < len; i++) {
            String newName = oldToNewNames.get(columns[i]);
            newColumns[i] = newName != null ? newName : columns[i];
        }

        return new Index(newColumns);
    }
}