package com.nhl.link.move.df.filter;

import com.nhl.link.move.df.DataRow;

@FunctionalInterface
public interface DataRowPredicate {

    boolean test(DataRow r);
}
