package com.nhl.link.etl;

import java.util.Date;

public class TimestampToken extends SyncToken {

	public TimestampToken(String name) {
		super(name, new Date());
	}

	public TimestampToken(String name, Date value) {
		super(name, value);
	}

	@Override
	public SyncToken getInitialToken() {
		return new TimestampToken(getName(), new Date(0l));
	}
}
