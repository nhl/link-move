package com.nhl.link.move.df;

import java.util.Map;

public class SparseIndex extends Index {

    protected SparseIndex(IndexPosition... positions) {
        super(positions);
    }

    @Override
    public Object[] compactCopy(Object[] row, Object[] to, int toOffset) {

        for (int i = 0; i < positions.length; i++) {
            to[toOffset + i] = positions[i].read(row);
        }

        return to;
    }

    public Index rename(Map<String, String> oldToNewNames) {

        int len = size();

        IndexPosition[] newPositions = new IndexPosition[len];
        for (int i = 0; i < len; i++) {
            IndexPosition pos = positions[i];
            String newName = oldToNewNames.get(pos.getName());
            newPositions[i] = newName != null ? new IndexPosition(pos.getPosition(), newName) : pos;
        }

        return new SparseIndex(newPositions);
    }
}
