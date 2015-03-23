package com.nhl.link.etl.runtime.listener;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.nhl.link.etl.CreateOrUpdateSegment;
import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.annotation.AfterSourceRowsConverted;
import com.nhl.link.etl.annotation.AfterSourcesMapped;
import com.nhl.link.etl.annotation.AfterTargetMatched;
import com.nhl.link.etl.annotation.AfterTargetMerged;

/**
 * @since 1.3
 */
public class CreateOrUpdateListenerFactory {

	@SuppressWarnings("unchecked")
	private static Class<? extends Annotation>[] LISTENER_ANNOTATIONS = new Class[] { AfterSourceRowsConverted.class,
			AfterSourcesMapped.class, AfterTargetMatched.class, AfterTargetMerged.class };

	private static MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

	public static void appendListeners(Map<Class<? extends Annotation>, List<CreateOrUpdateListener>> listeners,
			final Object listener) {

		try {
			doAppendListeners(listeners, listener);
		} catch (IllegalAccessException e) {
			throw new EtlRuntimeException("Error analyzing listener " + listener.getClass().getName(), e);
		}
	}

	public static void doAppendListeners(Map<Class<? extends Annotation>, List<CreateOrUpdateListener>> listeners,
			final Object listener) throws IllegalAccessException {
		for (Method m : listener.getClass().getDeclaredMethods()) {

			CreateOrUpdateListener listenerWrapper = null;

			for (final Class<? extends Annotation> at : LISTENER_ANNOTATIONS) {
				Annotation a = m.getAnnotation(at);
				if (a != null) {

					if (listenerWrapper == null) {
						final MethodHandle handle = LOOKUP.unreflect(m);

						listenerWrapper = new CreateOrUpdateListener() {

							@Override
							public void afterStageFinished(Execution exec, CreateOrUpdateSegment<?> segment) {
								try {
									handle.invoke(listener, exec, segment);
								} catch (Throwable e) {
									throw new EtlRuntimeException("Error invoking listener " + at.getSimpleName(), e);
								}
							}
						};
					}

					List<CreateOrUpdateListener> list = listeners.get(at);
					if (list == null) {
						list = new ArrayList<>();
						listeners.put(at, list);
					}

					list.add(listenerWrapper);
				}
			}
		}
	}
}
