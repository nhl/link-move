package com.nhl.link.move.df;

import com.nhl.link.move.df.zip.Zipper;

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

    public int[] positions(String... columnNames) {

        int len = columnNames.length;
        int[] positions = new int[len];

        for (int i = 0; i < len; i++) {
            positions[i] = position(columnNames[i]);
        }

        return positions;
    }

    public boolean hasColumn(String columnName) {
        if (columnIndex == null) {
            this.columnIndex = computeColumnIndex();
        }

        return columnIndex.containsKey(columnName);
    }

    public int position(String columnName) {
        if (columnIndex == null) {
            this.columnIndex = computeColumnIndex();
        }

        return columnIndex.computeIfAbsent(columnName, n -> {
            throw new IllegalArgumentException("Column '" + n + "' is not present in Columns");
        });
    }

    private Map<String, Integer> computeColumnIndex() {
        Map<String, Integer> index = new HashMap<>();

        for (int i = 0; i < columns.length; i++) {
            Integer previous = index.put(columns[i], i);
            if (previous != null) {
                throw new IllegalStateException("Duplicate column '" + columns[i] +
                        "'. Found at " + previous + " and " + i);
            }
        }
        return index;
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

    public Index addColumns(String... extraColumns) {
        return Zipper.zipIndex(this, new Index(extraColumns));
    }
}
