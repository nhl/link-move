package com.nhl.link.move.df.map;

import com.nhl.link.move.df.DataRow;

@FunctionalInterface
public interface DataRowMapper {

    static DataRowMapper identity() {
        return r -> r;
    }

    DataRow map(DataRow row);
}
