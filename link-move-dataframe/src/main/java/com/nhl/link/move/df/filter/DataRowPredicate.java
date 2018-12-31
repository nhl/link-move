package com.nhl.link.move.df.filter;

@FunctionalInterface
public interface DataRowPredicate {

    boolean test(Object[] r);
}
