package com.nhl.link.move;

/**
 * @deprecated as we are no longer planning to support {@link SyncToken}
 */
@Deprecated(since = "3.0.0", forRemoval = true)
public class IntToken extends SyncToken {

	public IntToken(String name, int value) {
		super(name, value);
	}

	@Override
	public SyncToken getInitialToken() {
		return new IntToken(getName(), 0);
	}
}
