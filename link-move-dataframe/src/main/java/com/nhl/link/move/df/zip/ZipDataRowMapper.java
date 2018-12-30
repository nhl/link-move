package com.nhl.link.move.df.zip;

import com.nhl.link.move.df.ArrayDataRow;
import com.nhl.link.move.df.DataRow;
import com.nhl.link.move.df.Index;
import com.nhl.link.move.df.map.DataRowMapper;

import java.util.Iterator;

public class ZipDataRowMapper implements DataRowMapper {

    private Iterator<DataRow> rows;

    public ZipDataRowMapper(Iterator<DataRow> rows) {
        this.rows = rows;
    }

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

    @Override
    public DataRow map(Index mappedIndex, DataRow row) {

        if (!rows.hasNext()) {
            throw new IllegalArgumentException("Right DataFrame is longer than the left");
        }

        return zip(mappedIndex, rows.next(), row);
    }

    private DataRow zip(Index mappedIndex, DataRow lr, DataRow rr) {
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
