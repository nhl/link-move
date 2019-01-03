package com.nhl.link.move.df;

import com.nhl.link.move.df.zip.Zipper;

import java.util.LinkedHashMap;
import java.util.Map;

public class Index {

    private String[] names;
    private Map<String, IndexPosition> positionsIndex;

    public Index(String... names) {
        this.names = names;
    }

    public String[] getNames() {
        return names;
    }

    public int size() {
        return names.length;
    }

    public IndexPosition[] positions(String... names) {

        int len = names.length;
        IndexPosition[] positions = new IndexPosition[len];

        for (int i = 0; i < len; i++) {
            positions[i] = position(names[i]);
        }

        return positions;
    }

    public IndexPosition position(String name) {
        if (positionsIndex == null) {
            this.positionsIndex = computePositions();
        }

        IndexPosition pos = positionsIndex.get(name);
        if (pos == null) {
            throw new IllegalArgumentException("Name '" + name + "' is not present in the Index");
        }

        return pos;
    }

    public boolean hasName(String names) {
        if (positionsIndex == null) {
            this.positionsIndex = computePositions();
        }

        return positionsIndex.containsKey(names);
    }

    private Map<String, IndexPosition> computePositions() {

        Map<String, IndexPosition> index = new LinkedHashMap<>();

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

        String[] newNames = new String[len];
        for (int i = 0; i < len; i++) {
            String newName = oldToNewNames.get(names[i]);
            newNames[i] = newName != null ? newName : names[i];
        }

        return new Index(newNames);
    }

    public Index addNames(String... extraNames) {
        return Zipper.zipIndex(this, new Index(extraNames));
    }
}
