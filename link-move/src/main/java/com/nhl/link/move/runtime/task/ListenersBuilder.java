package com.nhl.link.move.runtime.task;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.runtime.task.common.CallbackExecutor;
import com.nhl.link.move.runtime.task.common.DataSegment;
import com.nhl.link.move.runtime.task.common.TaskStageType;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @since 1.3
 */
public class ListenersBuilder<T extends DataSegment, S extends TaskStageType> {

	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

	private final Class<? extends Annotation>[] annotations;
	private final Map<Class<? extends Annotation>, List<StageListener>> listeners;
	private final Map<S, List<BiConsumer<Execution, T>>> callbacks;

	@SafeVarargs
	public ListenersBuilder(Class<? extends Annotation>... annotations) {
		this.callbacks = new HashMap<>();
		this.listeners = new HashMap<>();
		this.annotations = annotations;
	}

	public CallbackExecutor<S, T> getCallbackExecutor() {
		return new CallbackExecutor<>(listeners, callbacks);
	}

	/**
	 * @since 3.0
	 */
	public void addStageCallback(S stageType, BiConsumer<Execution, T> callback) {
        List<BiConsumer<Execution, T>> stageCallbacks = callbacks.computeIfAbsent(stageType, k -> new LinkedList<>());
        stageCallbacks.add(callback);
	}

	/**
	 * @deprecated use lambda-based callbacks instead, @see {@link com.nhl.link.move.runtime.task.ListenersBuilder#addStageCallback}
	 */
	@Deprecated
	public ListenersBuilder<T, S> addListener(Object listener) {

		try {
			doStageListener(listener);
		} catch (IllegalAccessException e) {
			throw new LmRuntimeException("Error analyzing listener " + listener.getClass().getName(), e);
		}

		return this;
	}

	private void doStageListener(final Object listener) throws IllegalAccessException {
		for (Method m : listener.getClass().getDeclaredMethods()) {

			StageListener listenerWrapper = null;

			for (final Class<? extends Annotation> at : annotations) {
				Annotation a = m.getAnnotation(at);
				if (a != null) {

					if (listenerWrapper == null) {
						final MethodHandle handle = LOOKUP.unreflect(m);

						listenerWrapper = (exec, segment) -> {
							try {
								handle.invoke(listener, exec, segment);
							} catch (Throwable e) {
								throw new LmRuntimeException("Error invoking listener " + at.getSimpleName(), e);
							}
						};
					}

					List<StageListener> list = listeners.computeIfAbsent(at, k -> new ArrayList<>());
					list.add(listenerWrapper);
				}
			}
		}
	}

	//visible for testing
	Map<Class<? extends Annotation>, List<StageListener>> getListeners() {
		return Map.copyOf(listeners);
	}
}
