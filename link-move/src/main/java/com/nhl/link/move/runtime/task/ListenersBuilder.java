package com.nhl.link.move.runtime.task;

import com.nhl.link.move.LmRuntimeException;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @since 1.3
 */
public class ListenersBuilder {

	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

	private final Class<? extends Annotation>[] annotations;
	private final Map<Class<? extends Annotation>, List<StageListener>> listeners;

	@SafeVarargs
	public ListenersBuilder(Class<? extends Annotation>... annotations) {
		this.listeners = new HashMap<>();
		this.annotations = annotations;
	}

	public Map<Class<? extends Annotation>, List<StageListener>> getListeners() {
		return listeners;
	}

	public ListenersBuilder addListener(Object listener) {

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
}
