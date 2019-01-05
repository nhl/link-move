package com.nhl.link.move.df.join;

import com.nhl.link.move.df.Index;

@FunctionalInterface
public interface IndexedJoinKeyMapper<V> {

    V map(Index columns, Object[] row);
}
