package com.nhl.link.move.df.map;

@FunctionalInterface
public interface ValueMapper<V, VR> {

    VR map(V value);
}
