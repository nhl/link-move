package com.nhl.link.move.runtime.task;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.nhl.link.move.LmTask;
import com.nhl.link.move.Execution;
import com.nhl.link.move.SyncToken;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import com.nhl.link.move.runtime.token.ITokenManager;

/**
 * @since 1.3
 */
public abstract class BaseTask implements LmTask {

	private ITokenManager tokenManager;

	public BaseTask(ITokenManager tokenManager) {
		this.tokenManager = tokenManager;
	}

	protected abstract void run(Execution execution, Map<String, ?> params);

	protected abstract String name();

	@Override
	public Execution run() {
		return run(Collections.emptyMap(), false);
	}

	@Override
	public Execution run(Map<String, ?> params) {
		return run(params, false);
	}

	@Override
	public Execution run(SyncToken token) {
		return run(token, Collections.emptyMap(), false);
	}

	@Override
	public Execution run(SyncToken token, Map<String, ?> params) {
		return run(token, params, false);
	}

	@Override
	public Execution dryRun() {
		return run(Collections.emptyMap(), true);
	}

	@Override
	public Execution dryRun(Map<String, ?> params) {
		return run(params, true);
	}

	@Override
	public Execution dryRun(SyncToken token) {
		return run(token, Collections.emptyMap(), true);
	}

	@Override
	public Execution dryRun(SyncToken token, Map<String, ?> params) {
		return run(token, params, true);
	}

	private Execution run(Map<String, ?> params, boolean dryRun) {
		if (params == null) {
			throw new NullPointerException("Null params");
		}

		try (Execution execution = new Execution(name(), params, dryRun)) {
			run(execution, params);
			return execution;
		}
	}

	private Execution run(SyncToken token, Map<String, ?> params, boolean dryRun) {
		Map<String, Object> combinedParams = new HashMap<>();

		SyncToken startToken = tokenManager.previousToken(token);
		combinedParams.put(LmRuntimeBuilder.START_TOKEN_VAR, startToken.getValue());
		combinedParams.put(LmRuntimeBuilder.END_TOKEN_VAR, token.getValue());

		combinedParams.putAll(params);

		Execution exec = run(combinedParams, dryRun);

		// if we ever start using delayed executions, token should be
		// saved inside the execution...
		tokenManager.saveToken(token);

		return exec;
	}
}
