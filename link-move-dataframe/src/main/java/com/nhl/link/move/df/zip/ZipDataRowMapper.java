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

    @Override
    public DataRow map(DataRow row) {

        if (!rows.hasNext()) {
            throw new IllegalArgumentException("Right DataFrame is longer than the left");
        }

        return zip(rows.next(), row);
    }

    private DataRow zip(DataRow lr, DataRow rr) {
        int llen = leftIndex.size();
        int rlen = index.size();

        String[] newIndex = new String[llen + rlen];

        System.arraycopy(leftIndex.getColumns(), 0, newIndex, 0, llen);

        String[] rColumns = index.getColumns();

        // resolve dupes on the right
        for(int i = 0; i < rlen; i++) {
            String column = rColumns[i];
            while (leftIndex.hasColumn(column)) {
                column = column + "_";
            }

            newIndex[i + llen] = column;
        }

        return new ArrayDataRow(newIndex);
    }
}
