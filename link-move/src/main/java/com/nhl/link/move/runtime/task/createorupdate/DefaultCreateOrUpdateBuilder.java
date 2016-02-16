package com.nhl.link.move.runtime.task.createorupdate;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Property;
import org.apache.cayenne.map.ObjEntity;

import com.nhl.link.move.CreateOrUpdateBuilder;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterSourcesMapped;
import com.nhl.link.move.annotation.AfterTargetsMatched;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.path.EntityPathNormalizer;
import com.nhl.link.move.runtime.path.IPathNormalizer;
import com.nhl.link.move.runtime.task.BaseTaskBuilder;
import com.nhl.link.move.runtime.task.ListenersBuilder;
import com.nhl.link.move.runtime.task.MapperBuilder;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.writer.ITargetPropertyWriterService;

/**
 * A builder of an ETL task that matches source data with target data based on a
 * certain unique attribute on both sides.
 */
public class DefaultCreateOrUpdateBuilder<T extends DataObject> extends BaseTaskBuilder
		implements CreateOrUpdateBuilder<T> {

	private IExtractorService extractorService;
	private ITargetCayenneService targetCayenneService;
	private ITokenManager tokenManager;
	private EntityPathNormalizer entityPathNormalizer;
	private MapperBuilder mapperBuilder;
	private Mapper mapper;
	private ListenersBuilder stageListenersBuilder;
	private Class<T> type;
	private ITargetPropertyWriterService writerService;

	private ExtractorName extractorName;

	public DefaultCreateOrUpdateBuilder(Class<T> type, ITargetCayenneService targetCayenneService,
			IExtractorService extractorService, ITokenManager tokenManager, IKeyAdapterFactory keyAdapterFactory,
			IPathNormalizer pathNormalizer, ITargetPropertyWriterService writerService) {

		this.type = type;
		this.targetCayenneService = targetCayenneService;
		this.extractorService = extractorService;
		this.tokenManager = tokenManager;

		ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);
		if (entity == null) {
			throw new LmRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
		}

		this.writerService = writerService;

		this.entityPathNormalizer = pathNormalizer.normalizer(entity);

		this.mapperBuilder = new MapperBuilder(entity, entityPathNormalizer, keyAdapterFactory);
		this.stageListenersBuilder = new ListenersBuilder(AfterSourceRowsConverted.class, AfterSourcesMapped.class,
				AfterTargetsMatched.class, AfterTargetsMerged.class);

		// always add stats listener..
		stageListener(CreateOrUpdateStatsListener.instance());
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> sourceExtractor(String location, String name) {
		this.extractorName = ExtractorName.create(location, name);
		return this;
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> sourceExtractor(String location) {
		// v.1 model style config
		return sourceExtractor(location, ExtractorModel.DEFAULT_NAME);
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> matchBy(Mapper mapper) {
		this.mapper = mapper;
		return this;
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> matchBy(String... keyAttributes) {
		this.mapper = null;
		this.mapperBuilder.matchBy(keyAttributes);
		return this;
	}

	/**
	 * @since 1.1
	 */
	@Override
	public DefaultCreateOrUpdateBuilder<T> matchBy(Property<?>... matchAttributes) {
		this.mapper = null;
		this.mapperBuilder.matchBy(matchAttributes);
		return this;
	}

	/**
	 * @since 1.4
	 */
	@Override
	public DefaultCreateOrUpdateBuilder<T> matchById() {
		this.mapper = null;
		this.mapperBuilder.matchById();
		return this;
	}

	@Override
	public DefaultCreateOrUpdateBuilder<T> batchSize(int batchSize) {
		this.batchSize = batchSize;
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
	public LmTask task() throws IllegalStateException {

		if (extractorName == null) {
			throw new IllegalStateException("Required 'extractorName' is not set");
		}

		return new CreateOrUpdateTask<T>(extractorName, batchSize, targetCayenneService, extractorService, tokenManager,
				createProcessor());
	}

	private CreateOrUpdateSegmentProcessor<T> createProcessor() {

		Mapper mapper = this.mapper != null ? this.mapper : mapperBuilder.build();

		SourceMapper sourceMapper = new SourceMapper(mapper);
		TargetMatcher<T> targetMatcher = new TargetMatcher<>(type, mapper);
		CreateOrUpdateMerger<T> merger = new CreateOrUpdateMerger<>(type, mapper, writerService.getWriterFactory(type));
		RowConverter rowConverter = new RowConverter(entityPathNormalizer);

		return new CreateOrUpdateSegmentProcessor<>(rowConverter, sourceMapper, targetMatcher, merger,
				stageListenersBuilder.getListeners());
	}

}
