package com.nhl.link.move.df;

import java.util.Objects;

@FunctionalInterface
public interface DataRowMapper {

    static <T> DataRowMapper identity() {
        return r -> r;
    }

    default DataRowMapper andThen(DataRowMapper after) {
        Objects.requireNonNull(after);
        return r -> after.apply(apply(r));
    }

    DataRow apply(DataRow row);
}
