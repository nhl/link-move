package com.nhl.link.move.runtime.jdbc;

import org.apache.cayenne.map.DbAttribute;

public interface JdbcNormalizer {

    Object normalize(Object value, DbAttribute targetAttribute);
}
