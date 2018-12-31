package com.nhl.link.move.df.filter;

import com.nhl.link.move.df.DataRow;

@FunctionalInterface
public interface DataRowJoinPredicate {

    boolean test(DataRow lr, DataRow rr);
}
