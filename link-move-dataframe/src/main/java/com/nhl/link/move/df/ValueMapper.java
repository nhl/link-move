package com.nhl.link.move.df;

@FunctionalInterface
public interface ValueMapper<V, VR> {

    VR apply(V value);
}
