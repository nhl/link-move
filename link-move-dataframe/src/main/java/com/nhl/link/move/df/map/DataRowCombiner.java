package com.nhl.link.move.df.map;

@FunctionalInterface
public interface DataRowCombiner {

    Object[] combine(Object[] lr, Object[] rr);
}
