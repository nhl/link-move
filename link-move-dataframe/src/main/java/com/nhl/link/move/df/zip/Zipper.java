package com.nhl.link.move.df.zip;

import com.nhl.link.move.df.Index;
import com.nhl.link.move.df.IndexPosition;
import com.nhl.link.move.df.map.DataRowCombiner;

public class Zipper {

    public static Index zipIndex(Index leftIndex, Index rightIndex) {

        int llen = leftIndex.size();
        int rlen = rightIndex.size();

        IndexPosition[] lPositions = leftIndex.getPositions();
        IndexPosition[] rPositions = rightIndex.getPositions();

        // zipped index is continuous to match rowZipper algorithm below that rebuilds the arrays, so reset left and
        // right positions, only preserve the names...

        IndexPosition[] zipped = new IndexPosition[llen + rlen];
        for (int i = 0; i < llen; i++) {
            zipped[i] = new IndexPosition(i, lPositions[i].getName());
        }

        // resolve dupes on the right
        for (int i = 0; i < rlen; i++) {

            String name = rPositions[i].getName();
            while (leftIndex.hasName(name)) {
                name = name + "_";
            }

            zipped[i + llen] = new IndexPosition(i + llen, name);
        }

        return Index.withPositions(zipped);
    }

    public static DataRowCombiner rowZipper(Index li, Index ri) {
        return (lr, rr) -> Zipper.zipRows(li, ri, lr, rr);
    }

    public static Object[] zipRows(Index li, Index ri, Object[] lr, Object[] rr) {

        Object[] zippedValues = new Object[li.size() + ri.size()];

        // rows can be null in case of outer joins...

        if (lr != null) {
            li.compactCopy(lr, zippedValues, 0);
        }

        if (rr != null) {
            ri.compactCopy(rr, zippedValues, zippedValues.length - ri.size());
        }

        return zippedValues;
    }
}
