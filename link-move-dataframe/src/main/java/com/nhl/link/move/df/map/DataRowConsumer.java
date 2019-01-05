package com.nhl.link.move.df.map;

import com.nhl.link.move.df.Index;

@FunctionalInterface
public interface DataRowConsumer {

    void consume(Index columns, Object[] row);
}
