package com.nhl.link.etl.batch.converter;

import com.nhl.link.etl.batch.BatchConverter;

/**
 * A {@link BatchConverter} that simply returns unchanged source when a target
 * is requested.
 * 
 * @since 1.3
 */
public class PassThroughConverter<S> implements BatchConverter<S, S> {

	private static final BatchConverter<?, ?> instance = new PassThroughConverter<>();

	@SuppressWarnings("unchecked")
	public static <S> BatchConverter<S, S> instance() {
		return (BatchConverter<S, S>) instance;
	}

	private PassThroughConverter() {
		// private noop constructor
	}

	@Override
	public S createTemplate() {
		return null;
	}

	@Override
	public S fromTemplate(S source, S targetTemplate) {
		return source;
	}
}
