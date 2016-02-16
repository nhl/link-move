package com.nhl.link.move.runtime.token;

import com.nhl.link.move.SyncToken;

public interface ITokenManager {

	/**
	 * Returns the last successfully processed token for a given task.
	 */
	SyncToken previousToken(SyncToken token);

	void saveToken(SyncToken token);
}
