package com.nhl.link.move.df.map;

import com.nhl.link.move.df.DataRow;

@FunctionalInterface
public interface DataRowCombiner {

    Object[] combine(DataRow lr, DataRow rr);
}
