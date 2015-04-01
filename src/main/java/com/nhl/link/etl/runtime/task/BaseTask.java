package com.nhl.link.etl.runtime.task;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.SyncToken;
import com.nhl.link.etl.runtime.EtlRuntimeBuilder;
import com.nhl.link.etl.runtime.token.ITokenManager;

/**
 * @since 1.3
 */
public abstract class BaseTask implements EtlTask {

	private ITokenManager tokenManager;

	public BaseTask(ITokenManager tokenManager) {
		this.tokenManager = tokenManager;
	}

	@Override
	public abstract Execution run(Map<String, ?> params);

	@Override
	public Execution run() {
		return run(Collections.<String, Object> emptyMap());
	}

	@Override
	public Execution run(SyncToken token) {
		return run(token, Collections.<String, Object> emptyMap());
	}

	@Override
	public Execution run(SyncToken token, Map<String, ?> params) {

		Map<String, Object> combinedParams = new HashMap<>();

		SyncToken startToken = tokenManager.previousToken(token);
		combinedParams.put(EtlRuntimeBuilder.START_TOKEN_VAR, startToken.getValue());
		combinedParams.put(EtlRuntimeBuilder.END_TOKEN_VAR, token.getValue());

		combinedParams.putAll(params);

		Execution exec = run(combinedParams);

		// if we ever start using delayed executions, token should be
		// saved inside the execution...
		tokenManager.saveToken(token);

		return exec;
	}

}
