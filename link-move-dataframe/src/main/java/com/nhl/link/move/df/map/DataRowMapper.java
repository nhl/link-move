package com.nhl.link.move.df.map;

import com.nhl.link.move.df.TransformContext;

@FunctionalInterface
public interface DataRowMapper {

    Object[] map(TransformContext c, Object[] row);
}
