package com.nhl.link.move.mapper;

import com.nhl.yadf.Index;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Expression;

/**
 * A strategy object for calculating a "key" from source and target objects of
 * the ETL. Keys are then used during the LOAD phase of the ETL execution to
 * match sources and targets.
 */
public interface Mapper {

	Object keyForTarget(DataObject target);

	Expression expressionForKey(Object key);

	Object keyForSource(Index index, Object[] source);
}
