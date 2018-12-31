package com.nhl.link.move.df.map;

import com.nhl.link.move.df.DataRow;

@FunctionalInterface
public interface DataRowMapper {

    static DataRowMapper valuesMapper() {
        return DataRow::copyValues;
    }

    Object[] map(DataRow row);
}
