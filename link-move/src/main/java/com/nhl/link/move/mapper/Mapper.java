package com.nhl.link.move.mapper;

import com.nhl.link.move.df.map.MapContext;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Expression;

import java.util.Map;

/**
 * A strategy object for calculating a "key" from source and target objects of
 * the ETL. Keys are then used during the LOAD phase of the ETL execution to
 * match sources and targets.
 */
public interface Mapper {

	Object keyForTarget(DataObject target);

	Expression expressionForKey(Object key);

	/**
	 * @since 2.7 switching to array based source. Use {@link #keyForSource(MapContext, Object[])}.
	 */
	@Deprecated
	Object keyForSource(Map<String, Object> source);

	Object keyForSource(MapContext context, Object[] source);
}
