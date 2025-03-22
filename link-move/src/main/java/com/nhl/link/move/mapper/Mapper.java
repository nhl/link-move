package com.nhl.link.move.mapper;

import org.apache.cayenne.exp.Expression;
import org.dflib.row.RowProxy;

/**
 * A strategy for calculating a "key" from source and target objects, so that an ETL pipeline could find matching
 * pairs of sources and targets.
 */
public interface Mapper {

    Object keyForTarget(Object target);

    Expression expressionForKey(Object key);

    Object keyForSource(RowProxy source);
}
