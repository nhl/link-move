package com.nhl.link.move.runtime.token;

import com.nhl.link.move.SyncToken;

/**
 * @deprecated as we are no longer planning to support {@link SyncToken}
 */
@Deprecated(since = "3.0.0", forRemoval = true)
public interface ITokenManager {

    /**
     * Returns the last successfully processed token for a given task.
     */
    SyncToken previousToken(SyncToken token);

    void saveToken(SyncToken token);
}
