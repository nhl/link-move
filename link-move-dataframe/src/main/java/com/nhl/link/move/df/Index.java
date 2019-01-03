package com.nhl.link.move.df;

import com.nhl.link.move.df.zip.Zipper;

import java.util.HashMap;
import java.util.Map;

public class Index {

    private String[] names;
    private Map<String, IndexPosition> nameIndex;

    public Index(String... names) {
        this.names = names;
    }

    public String[] getNames() {
        return names;
    }

    public int size() {
        return names.length;
    }

    public IndexPosition[] positions(String... columnNames) {

        int len = columnNames.length;
        IndexPosition[] positions = new IndexPosition[len];

        for (int i = 0; i < len; i++) {
            positions[i] = position(columnNames[i]);
        }

        return positions;
    }

    public IndexPosition position(String name) {
        if (nameIndex == null) {
            this.nameIndex = computeColumnIndex();
        }

        IndexPosition pos = nameIndex.get(name);
        if (pos == null) {
            throw new IllegalArgumentException("Name '" + name + "' is not present in the Index");
        }

        return pos;
    }

    public boolean hasColumn(String columnName) {
        if (nameIndex == null) {
            this.nameIndex = computeColumnIndex();
        }

        return nameIndex.containsKey(columnName);
    }

    private Map<String, IndexPosition> computeColumnIndex() {
        Map<String, IndexPosition> index = new HashMap<>();

        for (int i = 0; i < names.length; i++) {
            IndexPosition previous = index.put(names[i], new IndexPosition(i, names[i]));
            if (previous != null) {
                throw new IllegalStateException("Duplicate column '" + names[i] +
                        "'. Found at " + previous + " and " + i);
            }
        }
        return index;
    }

    public Index rename(Map<String, String> oldToNewNames) {

        int len = size();

        String[] newColumns = new String[len];
        for (int i = 0; i < len; i++) {
            String newName = oldToNewNames.get(names[i]);
            newColumns[i] = newName != null ? newName : names[i];
        }

        return new Index(newColumns);
    }

    public Index addColumns(String... extraColumns) {
        return Zipper.zipIndex(this, new Index(extraColumns));
    }
}
