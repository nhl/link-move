package com.nhl.link.move.df.map;

import com.nhl.link.move.df.DataRow;
import com.nhl.link.move.df.Index;

@FunctionalInterface
public interface DataRowMapper {

    static DataRowMapper identity() {
        return (i, r) -> r.reindex(i);
    }

    DataRow map(Index mappedIndex, DataRow row);
}
