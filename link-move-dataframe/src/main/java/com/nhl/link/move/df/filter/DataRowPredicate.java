package com.nhl.link.move.df.filter;

import com.nhl.link.move.df.Index;

@FunctionalInterface
public interface DataRowPredicate {

    boolean test(Index columns, Object[] r);
}
