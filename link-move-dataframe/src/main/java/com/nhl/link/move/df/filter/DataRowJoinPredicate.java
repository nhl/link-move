package com.nhl.link.move.df.filter;

@FunctionalInterface
public interface DataRowJoinPredicate {

    boolean test(Object[] lr, Object[] rr);
}
