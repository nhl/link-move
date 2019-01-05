package com.nhl.link.move.df.map;

@FunctionalInterface
public interface DataRowCombiner {

    Object[] combine(CombineContext context, Object[] lr, Object[] rr);
}
