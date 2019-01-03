package com.nhl.link.move.df.zip;

import com.nhl.link.move.df.DataRow;
import com.nhl.link.move.df.Index;
import com.nhl.link.move.df.map.DataRowCombiner;

public class Zipper {

    public static Index zipIndex(Index leftIndex, Index rightIndex) {

        int llen = leftIndex.size();
        int rlen = rightIndex.size();

        String[] zippedColumns = new String[llen + rlen];

        System.arraycopy(leftIndex.getNames(), 0, zippedColumns, 0, llen);

        String[] rColumns = rightIndex.getNames();

        // resolve dupes on the right
        for (int i = 0; i < rlen; i++) {
            String column = rColumns[i];
            while (leftIndex.hasColumn(column)) {
                column = column + "_";
            }

            zippedColumns[i + llen] = column;
        }

        return new Index(zippedColumns);
    }

    public static DataRowCombiner rowZipper(int zippedWidth) {
        return (lr, rr) -> Zipper.zipRows(zippedWidth, lr, rr);
    }

    public static Object[] zipRows(int zippedWidth, Object[] lr, Object[] rr) {

        // rows can be null in case of outer joins

        Object[] zippedValues = new Object[zippedWidth];

        if (lr != null) {
            DataRow.copyTo(lr, zippedValues, 0);
        }

        if (rr != null) {
            DataRow.copyTo(rr, zippedValues, zippedValues.length - rr.length);
        }

        return zippedValues;
    }
}
