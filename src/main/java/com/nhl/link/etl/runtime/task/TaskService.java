package com.nhl.link.etl.runtime.task;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.di.Inject;

import com.nhl.link.etl.CreateOrUpdateBuilder;
import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.mapper.IKeyAdapterFactory;
import com.nhl.link.etl.runtime.task.createorupdate.DefaultCreateOrUpdateBuilder;
import com.nhl.link.etl.runtime.token.ITokenManager;

public class TaskService implements ITaskService {

	private IExtractorService extractorService;
	private ITargetCayenneService targetCayenneService;
	private ITokenManager tokenManager;
	private IKeyAdapterFactory keyBuilderFactory;

	public TaskService(@Inject IExtractorService extractorService, @Inject ITargetCayenneService targetCayenneService,
			@Inject ITokenManager tokenManager, @Inject IKeyAdapterFactory keyBuilderFactory) {
		this.extractorService = extractorService;
		this.targetCayenneService = targetCayenneService;
		this.tokenManager = tokenManager;
		this.keyBuilderFactory = keyBuilderFactory;
	}

	@Override
	public <T extends DataObject> CreateOrUpdateBuilder<T> createOrUpdate(Class<T> type) {
		return new DefaultCreateOrUpdateBuilder<>(type, targetCayenneService, extractorService, tokenManager,
				keyBuilderFactory);
	}

	@Deprecated
	@Override
	public <T extends DataObject> CreateOrUpdateBuilder<T> createTaskBuilder(Class<T> type) {
		return createOrUpdate(type);
	}

}
