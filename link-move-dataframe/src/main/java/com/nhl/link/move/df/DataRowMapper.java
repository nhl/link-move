package com.nhl.link.move.df;

@FunctionalInterface
public interface DataRowMapper {

    static DataRowMapper identity() {
        return r -> r;
    }

    DataRow map(DataRow row);
}
