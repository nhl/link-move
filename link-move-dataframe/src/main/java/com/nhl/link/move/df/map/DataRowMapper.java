package com.nhl.link.move.df.map;

import com.nhl.link.move.df.DataRow;

@FunctionalInterface
public interface DataRowMapper {

    static DataRowMapper self() {
        return r -> r;
    }

    static DataRowMapper copy() {
        return DataRow::copy;
    }


    Object[] map(Object[] row);
}
