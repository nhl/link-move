package com.nhl.link.move.df.map;

import com.nhl.link.move.df.Index;

@FunctionalInterface
public interface IndexMapper {

    Index map(Index index);
}
