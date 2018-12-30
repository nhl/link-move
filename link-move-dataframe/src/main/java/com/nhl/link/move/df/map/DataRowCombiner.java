package com.nhl.link.move.df.map;

import com.nhl.link.move.df.DataRow;
import com.nhl.link.move.df.Index;

@FunctionalInterface
public interface DataRowCombiner {

    DataRow combine(Index mappedIndex, DataRow lr, DataRow rr);
}
