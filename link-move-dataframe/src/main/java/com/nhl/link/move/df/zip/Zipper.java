package com.nhl.link.move.df.zip;

import com.nhl.link.move.df.ArrayDataRow;
import com.nhl.link.move.df.DataRow;
import com.nhl.link.move.df.Index;

public class Zipper {

    public static Index zipIndex(Index leftIndex, Index rightIndex) {

        int llen = leftIndex.size();
        int rlen = rightIndex.size();

        String[] zippedColumns = new String[llen + rlen];

        System.arraycopy(leftIndex.getColumns(), 0, zippedColumns, 0, llen);

        String[] rColumns = rightIndex.getColumns();

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


    public static DataRow zipRows(Index mappedIndex, DataRow lr, DataRow rr) {

        int llen = lr.size();
        int rlen = rr.size();

        if (llen + rlen != mappedIndex.size()) {
            throw new IllegalArgumentException("Index size of "
                    + mappedIndex.size()
                    + " must be a sum of left and right row sizes ("
                    + llen + ", " + rlen + ")");
        }

        Object[] zippedValues = new Object[mappedIndex.size()];

        lr.copyTo(zippedValues, 0);
        rr.copyTo(zippedValues, llen);

        return new ArrayDataRow(mappedIndex, zippedValues);
    }
}
