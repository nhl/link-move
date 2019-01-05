package com.nhl.link.move.df;

import com.nhl.link.move.df.map.ValueMapper;

import java.util.Map;

public class SparseIndex extends Index {

    protected SparseIndex(IndexPosition... positions) {
        super(positions);
    }

    @Override
    public Index compactIndex() {
        String[] names = new String[positions.length];
        for (int i = 0; i < positions.length; i++) {
            names[i] = positions[i].name();
        }
        return Index.withNames(names);
    }

    @Override
    public Object[] compactCopy(Object[] row, Object[] to, int toOffset) {

        for (int i = 0; i < positions.length; i++) {
            to[toOffset + i] = positions[i].read(row);
        }

        return to;
    }

    @Override
    public <VR> Object[] mapColumn(Object[] row, IndexPosition position, ValueMapper<Object[], VR> m) {

        // since we are referencing a slot in the compact copy, we should use position's ordinal value instead of its
        // index against the original row..

        Object[] newValues = compactCopy(row, new Object[size()], 0);

        newValues[position.ordinal()] = m.map(row);
        return newValues;
    }

    public Index rename(Map<String, String> oldToNewNames) {

        int len = size();

        IndexPosition[] newPositions = new IndexPosition[len];
        for (int i = 0; i < len; i++) {
            IndexPosition pos = positions[i];
            String newName = oldToNewNames.get(pos.name());
            newPositions[i] = newName != null ? new IndexPosition(i, pos.rowIndex(), newName) : pos;
        }

        return new SparseIndex(newPositions);
    }
}
