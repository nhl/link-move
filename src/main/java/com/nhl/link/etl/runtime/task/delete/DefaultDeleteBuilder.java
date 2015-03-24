package com.nhl.link.etl.runtime.task.delete;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.Property;
import org.apache.cayenne.map.ObjEntity;

import com.nhl.link.etl.DeleteBuilder;
import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.annotation.AfterMissingTargetsFiltered;
import com.nhl.link.etl.annotation.AfterSourcesMapped;
import com.nhl.link.etl.mapper.Mapper;
import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.key.IKeyAdapterFactory;
import com.nhl.link.etl.runtime.task.BaseTaskBuilder;
import com.nhl.link.etl.runtime.task.ITaskService;
import com.nhl.link.etl.runtime.task.ListenersBuilder;
import com.nhl.link.etl.runtime.task.MapperBuilder;
import com.nhl.link.etl.runtime.token.ITokenManager;

/**
 * @since 1.3
 */
public class DefaultDeleteBuilder<T extends DataObject> extends BaseTaskBuilder implements DeleteBuilder<T> {

	private ITaskService taskService;
	private ITokenManager tokenManager;
	private ITargetCayenneService targetCayenneService;
	private Class<T> type;

	private Expression targetFilter;
	private String extractorName;
	private MapperBuilder mapperBuilder;
	private ListenersBuilder listenersBuilder;

	public DefaultDeleteBuilder(Class<T> type, ITargetCayenneService targetCayenneService,
			IKeyAdapterFactory keyAdapterFactory, ITaskService taskService) {

		this.taskService = taskService;
		this.targetCayenneService = targetCayenneService;
		this.type = type;

		ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);
		if (entity == null) {
			throw new EtlRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
		}

		this.mapperBuilder = new MapperBuilder(entity, keyAdapterFactory);
		this.listenersBuilder = new ListenersBuilder(AfterSourcesMapped.class, AfterMissingTargetsFiltered.class);

		// always add stats listener..
		stageListener(DeleteStatsListener.instance());
	}

	@Override
	public DefaultDeleteBuilder<T> stageListener(Object listener) {
		listenersBuilder.addListener(listener);
		return this;
	}

	@Override
	public DefaultDeleteBuilder<T> batchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	@Override
	public DefaultDeleteBuilder<T> targetFilter(Expression targetFilter) {
		this.targetFilter = targetFilter;
		return this;
	}

	@Override
	public DefaultDeleteBuilder<T> sourceMatchExtractor(String extractorName) {
		this.extractorName = extractorName;
		return this;
	}

	@Override
	public DefaultDeleteBuilder<T> matchBy(Mapper mapper) {
		mapperBuilder.matchBy(mapper);
		return this;
	}

	@Override
	public DefaultDeleteBuilder<T> matchBy(String... keyAttributes) {
		mapperBuilder.matchBy(keyAttributes);
		return this;
	}

	@Override
	public DefaultDeleteBuilder<T> matchBy(Property<?>... matchAttributes) {
		mapperBuilder.matchBy(matchAttributes);
		return this;
	}

	@Override
	public DefaultDeleteBuilder<T> matchById(String idProperty) {
		mapperBuilder.matchById(idProperty);
		return this;
	}

	@Override
	public DeleteTask<T> task() throws IllegalStateException {
		if (extractorName == null) {
			throw new IllegalStateException("Required 'sourceMatchExtractor' is not set");
		}

		return new DeleteTask<T>(extractorName, batchSize, type, targetFilter, targetCayenneService, tokenManager,
				createProcessor());
	}

	private DeleteSegmentProcessor<T> createProcessor() {
		Mapper mapper = mapperBuilder.build();

		EtlTask keysSubtask = taskService.extractSourceKeys().sourceExtractor(extractorName).matchBy(mapper).task();

		TargetMapper<T> targetMapper = new TargetMapper<>(mapper);
		MissingTargetsFilterStage<T> sourceMatcher = new MissingTargetsFilterStage<>(keysSubtask);
		DeleteTargetStage<T> deleter = new DeleteTargetStage<>();

		return new DeleteSegmentProcessor<>(targetMapper, sourceMatcher, deleter, listenersBuilder.getListeners());
	}
}
