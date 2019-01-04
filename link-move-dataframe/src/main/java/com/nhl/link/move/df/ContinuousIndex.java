package com.nhl.link.move.df;

import java.util.Map;

public class ContinuousIndex extends Index {

    protected ContinuousIndex(IndexPosition... positions) {
        super(positions);
    }

    public Object[] copyTo(Object[] row, Object[] to, int toOffset) {
        System.arraycopy(row, 0, to, toOffset, positions.length);
        return to;
    }

    public Index rename(Map<String, String> oldToNewNames) {

        int len = size();

        IndexPosition[] newPositions = new IndexPosition[len];
        for (int i = 0; i < len; i++) {
            IndexPosition pos = positions[i];
            String newName = oldToNewNames.get(pos.getName());
            newPositions[i] = newName != null ? new IndexPosition(i, newName) : pos;
        }

        return new ContinuousIndex(newPositions);
    }
}
