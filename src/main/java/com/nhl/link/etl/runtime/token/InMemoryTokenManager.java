package com.nhl.link.etl.runtime.token;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.nhl.link.etl.SyncToken;

/**
 * A simple token manager that stores a map of task tokens in memory. An
 * implementation in a real ETL runtime must use some form of persistent store.
 */
public class InMemoryTokenManager implements ITokenManager {

	private ConcurrentMap<String, SyncToken> tokens;

	public InMemoryTokenManager() {
		this.tokens = new ConcurrentHashMap<>();
	}

	@Override
	public SyncToken previousToken(SyncToken token) {

		SyncToken previousToken = tokens.get(token.getName());
		if (previousToken == null) {
			SyncToken newToken = token.getInitialToken();

			// even though we are pretending we are trying to avoid race
			// conditions here, the current token API is not designed to
			// reliably assign non-conflicting token ranges to tasks, so a task
			// may still end up processing the same range as another task.
			// Callers must ensure that the same task is only run serially
			SyncToken existing = tokens.putIfAbsent(token.getName(), newToken);
			previousToken = existing != null ? existing : newToken;
		}

		return previousToken;
	}

	@Override
	public void saveToken(SyncToken token) {
		tokens.put(token.getName(), token);
	}

}
