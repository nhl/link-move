package com.nhl.link.move.runtime.task.delete;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cayenne.ObjectContext;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysTask;

/**
 * @since 1.3
 */
public class MissingTargetsFilterStage<T> {

	public static final String SOURCE_KEYS_KEY = MissingTargetsFilterStage.class.getName() + ".SOURCE_KEYS";

	private LmTask keysSubtask;

	public MissingTargetsFilterStage(LmTask keysSubtask) {
		this.keysSubtask = keysSubtask;
	}

	public List<T> filterMissing(Execution parentExec, ObjectContext context, Map<Object, T> mappedTargets) {

		Set<Object> sourceKeys = sourceKeys(parentExec);
		List<T> matched = new ArrayList<>();

		for (Entry<Object, T> e : mappedTargets.entrySet()) {
			if (!sourceKeys.contains(e.getKey())) {
				matched.add(e.getValue());
			}
		}

		return matched;
	}

	@SuppressWarnings("unchecked")
	private Set<Object> sourceKeys(Execution parentExec) {

		// cache keys in parent execution...
		Set<Object> keys = (Set<Object>) parentExec.getAttribute(SOURCE_KEYS_KEY);
		if (keys == null) {
			keys = loadKeys(parentExec);
			parentExec.setAttribute(SOURCE_KEYS_KEY, keys);
		}

		return keys;
	}

	@SuppressWarnings("unchecked")
	private Set<Object> loadKeys(Execution parentExec) {
		Execution exec = keysSubtask.run(parentExec.getParameters());

		parentExec.getStats().incrementExtracted(exec.getStats().getExtracted());

		Set<Object> keys = (Set<Object>) exec.getAttribute(SourceKeysTask.RESULT_KEY);
		if (keys == null) {
			throw new LmRuntimeException("Unxpected state of keys subtask. No attribute for key: "
					+ SourceKeysTask.RESULT_KEY);
		}

		return keys;
	}

}
