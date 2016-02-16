package com.nhl.link.move.runtime.task.delete;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cayenne.ObjectContext;

import com.nhl.link.move.Execution;

/**
 * @since 1.3
 */
public class MissingTargetsFilterStage<T> {

	public List<T> filterMissing(Execution parentExec, ObjectContext context, Map<Object, T> mappedTargets,
			Set<Object> sourceKeys) {

		List<T> matched = new ArrayList<>();

		for (Entry<Object, T> e : mappedTargets.entrySet()) {
			if (!sourceKeys.contains(e.getKey())) {
				matched.add(e.getValue());
			}
		}

		return matched;
	}

}
