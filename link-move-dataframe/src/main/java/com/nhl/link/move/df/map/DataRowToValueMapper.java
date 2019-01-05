package com.nhl.link.move.df.map;

import com.nhl.link.move.df.TransformContext;

@FunctionalInterface
public interface DataRowToValueMapper<V> {

    V map(TransformContext c, Object[] row);
}
