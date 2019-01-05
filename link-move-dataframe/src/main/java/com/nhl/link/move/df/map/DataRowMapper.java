package com.nhl.link.move.df.map;

@FunctionalInterface
public interface DataRowMapper {

    Object[] map(MapContext c, Object[] row);
}
