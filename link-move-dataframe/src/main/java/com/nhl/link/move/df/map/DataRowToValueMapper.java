package com.nhl.link.move.df.map;

@FunctionalInterface
public interface DataRowToValueMapper<V> {

    V map(MapContext c, Object[] row);
}
