package com.nhl.link.framework.etl;

public class IntToken extends SyncToken {

	public IntToken(String name, int value) {
		super(name, value);
	}

	@Override
	public SyncToken getInitialToken() {
		return new IntToken(getName(), 0);
	}
}
