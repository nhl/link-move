package com.nhl.link.move.runtime.task;

import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.common.CallbackExecutor;
import com.nhl.link.move.runtime.task.common.DataSegment;
import com.nhl.link.move.runtime.task.common.TaskStageType;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @since 1.3
 */
public class ListenersBuilder<T extends DataSegment, S extends TaskStageType> {

	private final Map<S, List<BiConsumer<Execution, T>>> callbacks;

	/**
	 * @since 4.0.0
	 */
	public ListenersBuilder() {
		this.callbacks = new HashMap<>();
	}

	public CallbackExecutor<S, T> getCallbackExecutor() {
		return new CallbackExecutor<>(callbacks);
	}

	/**
	 * @since 3.0.0
	 */
	public void addStageCallback(S stageType, BiConsumer<Execution, T> callback) {
        List<BiConsumer<Execution, T>> stageCallbacks = callbacks.computeIfAbsent(stageType, k -> new LinkedList<>());
        stageCallbacks.add(callback);
	}
}
