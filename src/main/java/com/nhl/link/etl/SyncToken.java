package com.nhl.link.etl;

/**
 * An opaque marker that identifies a place in the source data set where the
 * synchronization stopped the last time. This can be a timestamp, a number or
 * any other way to identify a position in the data set to start a new sync.
 */
public abstract class SyncToken {

	public static SyncToken nullToken(String name) {
		return new SyncToken(name, null) {
			@Override
			public SyncToken getInitialToken() {
				return this;
			}
		};
	}

	private String name;
	private Object value;

	public SyncToken(String name, Object value) {
		this.name = name;
		this.value = value;
	}

	/**
	 * Returns a starting point for this token type. E.g. if a token is a
	 * timestamp, this may return a timestamp corresponding to 1970-01-01.
	 */
	public abstract SyncToken getInitialToken();

	public Object getValue() {
		return value;
	}

	public String getName() {
		return name;
	}
}
