package com.nhl.link.etl.runtime.task.createorupdate;

import java.util.ArrayList;
import java.util.List;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Property;
import org.apache.cayenne.map.ObjEntity;

import com.nhl.link.etl.CreateOrUpdateBuilder;
import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.annotation.AfterSourceRowsConverted;
import com.nhl.link.etl.annotation.AfterSourcesMapped;
import com.nhl.link.etl.annotation.AfterTargetsMatched;
import com.nhl.link.etl.annotation.AfterTargetsMerged;
import com.nhl.link.etl.load.LoadListener;
import com.nhl.link.etl.mapper.Mapper;
import com.nhl.link.etl.metadata.RelationshipInfo;
import com.nhl.link.etl.metadata.RelationshipType;
import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.key.IKeyAdapterFactory;
import com.nhl.link.etl.runtime.task.BaseTaskBuilder;
import com.nhl.link.etl.runtime.task.MapperBuilder;
import com.nhl.link.etl.runtime.task.ListenersBuilder;
import com.nhl.link.etl.runtime.token.ITokenManager;

/**
 * A builder of an ETL task that matches source data with target data based on a
 * certain unique attribute on both sides.
 */
@SuppressWarnings("deprecation")
public class DefaultCreateOrUpdateBuilder<T extends DataObject> extends BaseTaskBuilder implements
		CreateOrUpdateBuilder<T> {

	private IExtractorService extractorService;
	private ITargetCayenneService targetCayenneService;
	private ITokenManager tokenManager;
	private MapperBuilder mapperBuilder;
	private ListenersBuilder stageListenersBuilder;
	private Class<T> type;

	private String extractorName;
	private List<RelationshipInfo> relationships;

	@Deprecated
	private List<LoadListener<T>> loadListeners;

	public DefaultCreateOrUpdateBuilder(Class<T> type, ITargetCayenneService targetCayenneService,
			IExtractorService extractorService, ITokenManager tokenManager, IKeyAdapterFactory keyAdapterFactory) {

		this.type = type;
		this.targetCayenneService = targetCayenneService;
		this.extractorService = extractorService;
		this.tokenManager = tokenManager;
		this.relationships = new ArrayList<>();

		ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);
		if (entity == null) {
			throw new EtlRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
		}
		this.mapperBuilder = new MapperBuilder(entity, keyAdapterFactory);
		this.stageListenersBuilder = new ListenersBuilder(AfterSourceRowsConverted.class,
				AfterSourcesMapped.class, AfterTargetsMatched.class, AfterTargetsMerged.class);

		// always add stats listener..
		stageListener(CreateOrUpdateStatsListener.instance());

		this.loadListeners = new ArrayList<>();
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> sourceExtractor(String extractorName) {
		this.extractorName = extractorName;
		return this;
	}

	@Deprecated
	@Override
	public CreateOrUpdateBuilder<T> withExtractor(String extractorName) {
		return sourceExtractor(extractorName);
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> matchBy(Mapper mapper) {
		mapperBuilder.matchBy(mapper);
		return this;
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> matchBy(String... keyAttributes) {
		mapperBuilder.matchBy(keyAttributes);
		return this;
	}

	/**
	 * @since 1.1
	 */
	@Override
	public DefaultCreateOrUpdateBuilder<T> matchBy(Property<?>... matchAttributes) {
		mapperBuilder.matchBy(matchAttributes);
		return this;
	}

	/**
	 * @since 1.1
	 */
	@Override
	public DefaultCreateOrUpdateBuilder<T> matchById(String idProperty) {
		mapperBuilder.matchById(idProperty);
		return this;
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> batchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	@Deprecated
	@Override
	public CreateOrUpdateBuilder<T> withBatchSize(int batchSize) {
		return batchSize(batchSize);
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> withToOneRelationship(String name,
			Class<? extends DataObject> relatedObjType, String keyAttribute) {
		this.relationships.add(new RelationshipInfo(name, keyAttribute, RelationshipType.TO_ONE, relatedObjType));
		return this;
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> withToManyRelationship(String name,
			Class<? extends DataObject> relatedObjType, String keyAttribute) {
		this.relationships.add(new RelationshipInfo(name, keyAttribute, RelationshipType.TO_MANY, relatedObjType));
		return this;
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> withToOneRelationship(String name,
			Class<? extends DataObject> relatedObjType, String keyAttribute, String relationshipKeyAttribute) {
		this.relationships.add(new RelationshipInfo(name, keyAttribute, RelationshipType.TO_ONE, relatedObjType,
				relationshipKeyAttribute));
		return this;
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> withToManyRelationship(String name,
			Class<? extends DataObject> relatedObjType, String keyAttribute, String relationshipKeyAttribute) {
		this.relationships.add(new RelationshipInfo(name, keyAttribute, RelationshipType.TO_MANY, relatedObjType,
				relationshipKeyAttribute));
		return this;
	}

	@Deprecated
	@Override
	public CreateOrUpdateBuilder<T> withListener(LoadListener<T> listener) {
		this.loadListeners.add(listener);
		return this;
	}

	/**
	 * @since 1.3
	 */
	@Override
	public CreateOrUpdateBuilder<T> stageListener(Object listener) {
		stageListenersBuilder.addListener(listener);
		return this;
	}

	@Override
	public EtlTask task() throws IllegalStateException {

		if (extractorName == null) {
			throw new IllegalStateException("Required 'extractorName' is not set");
		}

		return new CreateOrUpdateTask<T>(extractorName, batchSize, targetCayenneService, extractorService,
				tokenManager, createProcessor());
	}

	private CreateOrUpdateSegmentProcessor<T> createProcessor() {

		Mapper mapper = mapperBuilder.build();
		CreateOrUpdateStrategy<T> createOrUpdateStrategy = createCreateOrUpdateStrategy();

		SourceMapper sourceMapper = new SourceMapper(mapper);
		TargetMatcher<T> targetMatcher = new TargetMatcher<>(type, mapper);
		CreateOrUpdateMerger<T> merger = new CreateOrUpdateMerger<>(type, mapper, createOrUpdateStrategy);

		return new CreateOrUpdateSegmentProcessor<>(RowConverter.instance(), sourceMapper, targetMatcher, merger,
				stageListenersBuilder.getListeners(), loadListeners);
	}

	private CreateOrUpdateStrategy<T> createCreateOrUpdateStrategy() {
		if (mapperBuilder.isById()) {
			return new CayenneCreateOrUpdateWithPKStrategy<>(relationships, mapperBuilder.getSingleMatchAttribute());
		} else {
			return new CayenneCreateOrUpdateStrategy<>(relationships);
		}
	}

}
