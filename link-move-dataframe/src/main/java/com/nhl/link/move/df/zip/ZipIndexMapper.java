package com.nhl.link.move.df.zip;

import com.nhl.link.move.df.Index;
import com.nhl.link.move.df.map.IndexMapper;

public class ZipIndexMapper implements IndexMapper {

    private Index leftIndex;

    public ZipIndexMapper(Index leftIndex) {
        this.leftIndex = leftIndex;
    }

    @Override
    public Index map(Index index) {

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

        return new Index(newIndex);
    }
}
