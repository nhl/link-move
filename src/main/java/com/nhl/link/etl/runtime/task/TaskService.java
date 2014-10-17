package com.nhl.link.etl.runtime.task;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.di.Inject;

import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.map.key.IKeyMapAdapterFactory;
import com.nhl.link.etl.runtime.token.ITokenManager;

public class TaskService implements ITaskService {

	private IExtractorService extractorService;
	private ITargetCayenneService targetCayenneService;
	private ITokenManager tokenManager;
	private IKeyMapAdapterFactory keyBuilderFactory;

	public TaskService(@Inject IExtractorService extractorService, @Inject ITargetCayenneService targetCayenneService,
			@Inject ITokenManager tokenManager, @Inject IKeyMapAdapterFactory keyBuilderFactory) {
		this.extractorService = extractorService;
		this.targetCayenneService = targetCayenneService;
		this.tokenManager = tokenManager;
		this.keyBuilderFactory = keyBuilderFactory;
	}

	@Override
	public <T extends DataObject> MatchingTaskBuilder<T> createTaskBuilder(Class<T> type) {
		return new MatchingTaskBuilder<>(type, targetCayenneService, extractorService, tokenManager, keyBuilderFactory);
	}

}
