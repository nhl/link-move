package com.nhl.link.etl.runtime.task.createorupdate;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Property;

import com.nhl.link.etl.CreateOrUpdateBuilder;
import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.load.LoadListener;
import com.nhl.link.etl.mapper.Mapper;
import com.nhl.link.etl.metadata.RelationshipInfo;
import com.nhl.link.etl.metadata.RelationshipType;
import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.key.IKeyAdapterFactory;
import com.nhl.link.etl.runtime.listener.CreateOrUpdateListener;
import com.nhl.link.etl.runtime.listener.CreateOrUpdateListenerFactory;
import com.nhl.link.etl.runtime.task.MappingTaskBuilder;
import com.nhl.link.etl.runtime.token.ITokenManager;

/**
 * A builder of an ETL task that matches source data with target data based on a
 * certain unique attribute on both sides.
 */
@SuppressWarnings("deprecation")
public class DefaultCreateOrUpdateBuilder<T extends DataObject> extends MappingTaskBuilder<T> implements
		CreateOrUpdateBuilder<T> {

	private IExtractorService extractorService;
	private ITokenManager tokenManager;

	private String extractorName;
	private List<RelationshipInfo> relationships;

	@Deprecated
	private List<LoadListener<T>> loadListeners;
	private Map<Class<? extends Annotation>, List<CreateOrUpdateListener>> stageListeners;

	public DefaultCreateOrUpdateBuilder(Class<T> type, ITargetCayenneService targetCayenneService,
			IExtractorService extractorService, ITokenManager tokenManager, IKeyAdapterFactory keyAdapterFactory) {

		super(type, targetCayenneService, keyAdapterFactory);

		this.extractorService = extractorService;
		this.tokenManager = tokenManager;
		this.relationships = new ArrayList<>();
		this.stageListeners = new HashMap<>();

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
	public DefaultCreateOrUpdateBuilder<T> matchBy(Mapper<T> mapper) {
		setMapper(mapper);
		return this;
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> matchBy(String... keyAttributes) {
		setMapperAttributeNames(keyAttributes);
		return this;
	}

	/**
	 * @since 1.1
	 */
	@Override
	public DefaultCreateOrUpdateBuilder<T> matchBy(Property<?>... matchAttributes) {
		setMapperProperties(matchAttributes);
		return this;
	}

	/**
	 * @since 1.1
	 */
	@Override
	public DefaultCreateOrUpdateBuilder<T> matchById(String idProperty) {
		setMapperId(idProperty);
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
		CreateOrUpdateListenerFactory.appendListeners(stageListeners, listener);
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

		Mapper<T> mapper = createMapper();
		CreateOrUpdateStrategy<T> createOrUpdateStrategy = createCreateOrUpdateStrategy();

		SourceMapper<T> sourceMapper = new SourceMapper<>(mapper);
		TargetMatcher<T> targetMatcher = new TargetMatcher<>(type, mapper);
		CreateOrUpdateMerger<T> merger = new CreateOrUpdateMerger<>(type, mapper, createOrUpdateStrategy);

		return new CreateOrUpdateSegmentProcessor<>(RowConverter.instance(), sourceMapper, targetMatcher, merger,
				stageListeners, loadListeners);
	}

	private CreateOrUpdateStrategy<T> createCreateOrUpdateStrategy() {
		if (byId) {
			return new CayenneCreateOrUpdateWithPKStrategy<>(relationships, getSingleMatchAttribute());
		} else {
			return new CayenneCreateOrUpdateStrategy<>(relationships);
		}
	}

}
