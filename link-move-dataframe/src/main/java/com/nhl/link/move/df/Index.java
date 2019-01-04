package com.nhl.link.move.df;

import com.nhl.link.move.df.map.ValueMapper;
import com.nhl.link.move.df.zip.Zipper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An "index" of the DataFrame that provides access to column (and in the future potentially row) metadata.
 */
public abstract class Index {

    protected IndexPosition[] positions;
    private Map<String, IndexPosition> positionsIndex;

    protected Index(IndexPosition... positions) {
        this.positions = positions;
    }

    protected static IndexPosition[] continuousPositions(String... names) {
        IndexPosition[] positions = new IndexPosition[names.length];
        for (int i = 0; i < names.length; i++) {
            positions[i] = new IndexPosition(i, names[i]);
        }

        return positions;
    }

    protected static boolean isContinuous(IndexPosition... positions) {

        // true if starts with zero and increments by one

        if (positions.length > 0 && positions[0].getPosition() > 0) {
            return false;
        }

        for (int i = 1; i < positions.length; i++) {
            if (positions[i].getPosition() != positions[i - 1].getPosition() + 1) {
                return false;
            }
        }

        return true;
    }

    public static Index withNames(String... names) {
        return new ContinuousIndex(continuousPositions(names));
    }

    public static Index withPositions(IndexPosition... positions) {
        return isContinuous(positions) ? new ContinuousIndex(positions) : new SparseIndex(positions);
    }

    public abstract Object[] compactCopy(Object[] row, Object[] to, int toOffset);

    public abstract Index rename(Map<String, String> oldToNewNames);

    public abstract Index compactIndex();

    public Index addNames(String... extraNames) {
        return Zipper.zipIndex(this, withNames(extraNames));
    }

    public <VR> Object[] addValues(Object[] row, ValueMapper<Object[], VR>... valueProducers) {

        int oldWidth = size();
        int expansionWidth = valueProducers.length;

        Object[] expandedRow = compactCopy(row, new Object[oldWidth + expansionWidth], 0);

        for (int i = 0; i < expansionWidth; i++) {
            expandedRow[oldWidth + i] = valueProducers[i].map(row);
        }

        return expandedRow;
    }

    public <VR> Object[] mapColumn(Object[] row, String columnName, ValueMapper<Object[], VR> m) {
        return mapColumn(row, position(columnName), m);
    }

    public abstract <VR> Object[] mapColumn(Object[] row, IndexPosition position, ValueMapper<Object[], VR> m);

    public Index dropNames(String... names) {

        if (names.length == 0) {
            return this;
        }

        if (positionsIndex == null) {
            this.positionsIndex = computePositions();
        }

        List<IndexPosition> toDrop = new ArrayList<>(names.length);
        for (String n : names) {
            IndexPosition ip = positionsIndex.get(n);
            if (ip != null) {
                toDrop.add(ip);
            }
        }

        if (toDrop.isEmpty()) {
            return this;
        }

        IndexPosition[] toKeep = new IndexPosition[size() - toDrop.size()];
        for (int i = 0, j = 0; i < positions.length; i++) {

            if (!toDrop.contains(positions[i])) {
                toKeep[j++] = positions[i];
            }
        }

        return Index.withPositions(toKeep);
    }

    public IndexPosition[] getPositions() {
        return positions;
    }

    public int size() {
        return positions.length;
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

        for (int i = 0; i < positions.length; i++) {
            IndexPosition previous = index.put(positions[i].getName(), positions[i]);
            if (previous != null) {
                throw new IllegalStateException("Duplicate position name '"
                        + positions[i].getName()
                        + "'. Found at " + previous + " and " + i);
            }
        }
        return index;
    }
}
