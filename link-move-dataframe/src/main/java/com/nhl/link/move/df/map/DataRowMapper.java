package com.nhl.link.move.df.map;

@FunctionalInterface
public interface DataRowMapper {

    static DataRowMapper self() {
        return r -> r;
    }

    Object[] map(Object[] row);
}
