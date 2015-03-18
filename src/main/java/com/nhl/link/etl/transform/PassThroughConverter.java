package com.nhl.link.etl.transform;

import com.nhl.link.etl.batch.BatchConverter;

/**
 * A {@link BatchConverter} that simply returns unchanged source when a target
 * is requested.
 * 
 * @since 1.3
 */
public class PassThroughConverter<T> implements BatchConverter<T, T> {

	private static final BatchConverter<?, ?> instance = new PassThroughConverter<>();

	@SuppressWarnings("unchecked")
	public static <T> BatchConverter<T, T> instance() {
		return (BatchConverter<T, T>) instance;
	}

	private PassThroughConverter() {
		// private noop constructor
	}

	@Override
	public T createTemplate() {
		return null;
	}

	@Override
	public T fromTemplate(T source, T targetTemplate) {
		return source;
	}
}
