package com.nhl.link.move.df.map;

import com.nhl.link.move.df.DataRow;

@FunctionalInterface
public interface DataRowToValueMapper<V> {

    V map(DataRow row);
}
