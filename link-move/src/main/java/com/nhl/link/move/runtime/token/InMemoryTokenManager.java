package com.nhl.link.move.runtime.token;

import com.nhl.link.move.SyncToken;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A simple token manager that stores a map of task tokens in memory. An
 * implementation of a real-life token manager should probably use some form of
 * persistent store to preserve the last token position between the container
 * restarts.
 */
@Deprecated(since = "3.0")
public class InMemoryTokenManager implements ITokenManager {

    private Map<String, SyncToken> tokens;

    public InMemoryTokenManager() {
        this.tokens = new ConcurrentHashMap<>();
    }

    @Override
    public SyncToken previousToken(SyncToken token) {
        return tokens.computeIfAbsent(token.getName(), n -> token.getInitialToken());
    }

    @Override
    public void saveToken(SyncToken token) {
        tokens.put(token.getName(), token);
    }

}
