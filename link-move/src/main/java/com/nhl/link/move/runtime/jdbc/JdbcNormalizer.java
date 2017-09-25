package com.nhl.link.move.runtime.jdbc;

import org.apache.cayenne.map.DbAttribute;

public interface JdbcNormalizer<T> {

    T normalize(Object value, DbAttribute targetAttribute);
}
