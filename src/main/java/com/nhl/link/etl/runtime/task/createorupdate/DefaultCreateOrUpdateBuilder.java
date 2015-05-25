package com.nhl.link.etl.runtime.task.createorupdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Property;
import org.apache.cayenne.exp.parser.ASTDbPath;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbJoin;
import org.apache.cayenne.map.DbRelationship;
import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.map.ObjEntity;
import org.apache.cayenne.reflect.AttributeProperty;
import org.apache.cayenne.reflect.ClassDescriptor;
import org.apache.cayenne.reflect.PropertyVisitor;
import org.apache.cayenne.reflect.ToManyProperty;
import org.apache.cayenne.reflect.ToOneProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nhl.link.etl.CreateOrUpdateBuilder;
import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.annotation.AfterSourceRowsConverted;
import com.nhl.link.etl.annotation.AfterSourcesMapped;
import com.nhl.link.etl.annotation.AfterTargetsMatched;
import com.nhl.link.etl.annotation.AfterTargetsMerged;
import com.nhl.link.etl.load.LoadListener;
import com.nhl.link.etl.mapper.Mapper;
import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.key.IKeyAdapterFactory;
import com.nhl.link.etl.runtime.task.BaseTaskBuilder;
import com.nhl.link.etl.runtime.task.ListenersBuilder;
import com.nhl.link.etl.runtime.task.MapperBuilder;
import com.nhl.link.etl.runtime.token.ITokenManager;
import com.nhl.link.etl.writer.TargetAttributePropertyWriter;
import com.nhl.link.etl.writer.TargetPkPropertyWriter;
import com.nhl.link.etl.writer.TargetPropertyWriter;
import com.nhl.link.etl.writer.TargetToManyPropertyWriter;
import com.nhl.link.etl.writer.TargetToOnePropertyWriter;

/**
 * A builder of an ETL task that matches source data with target data based on a
 * certain unique attribute on both sides.
 */
@SuppressWarnings("deprecation")
public class DefaultCreateOrUpdateBuilder<T extends DataObject> extends BaseTaskBuilder implements
		CreateOrUpdateBuilder<T> {

	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCreateOrUpdateBuilder.class);

	private IExtractorService extractorService;
	private ITargetCayenneService targetCayenneService;
	private ITokenManager tokenManager;
	private MapperBuilder mapperBuilder;
	private Mapper mapper;
	private ListenersBuilder stageListenersBuilder;
	private Class<T> type;

	private String extractorName;

	@Deprecated
	private List<LoadListener<T>> loadListeners;

	public DefaultCreateOrUpdateBuilder(Class<T> type, ITargetCayenneService targetCayenneService,
			IExtractorService extractorService, ITokenManager tokenManager, IKeyAdapterFactory keyAdapterFactory) {

		this.type = type;
		this.targetCayenneService = targetCayenneService;
		this.extractorService = extractorService;
		this.tokenManager = tokenManager;

		ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);
		if (entity == null) {
			throw new EtlRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
		}
		this.mapperBuilder = new MapperBuilder(entity, keyAdapterFactory);
		this.stageListenersBuilder = new ListenersBuilder(AfterSourceRowsConverted.class, AfterSourcesMapped.class,
				AfterTargetsMatched.class, AfterTargetsMerged.class);

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

	@Deprecated
	@Override
	public CreateOrUpdateBuilder<T> withBatchSize(int batchSize) {
		return batchSize(batchSize);
	}

	/**
	 * Does nothing.
	 * 
	 * @deprecated since 1.4
	 */
	@Deprecated
	@Override
	public DefaultCreateOrUpdateBuilder<T> withToOneRelationship(String name,
			Class<? extends DataObject> relatedObjType, String keyAttribute) {
		return this;
	}

	/**
	 * Does nothing.
	 * 
	 * @deprecated since 1.4
	 */
	@Deprecated
	@Override
	public DefaultCreateOrUpdateBuilder<T> withToManyRelationship(String name,
			Class<? extends DataObject> relatedObjType, String keyAttribute) {
		return this;
	}

	/**
	 * Does nothing.
	 * 
	 * @deprecated since 1.4
	 */
	@Deprecated
	@Override
	public DefaultCreateOrUpdateBuilder<T> withToOneRelationship(String name,
			Class<? extends DataObject> relatedObjType, String keyAttribute, String relationshipKeyAttribute) {
		return this;
	}

	/**
	 * Does nothing.
	 * 
	 * @deprecated since 1.4
	 */
	@Deprecated
	@Override
	public DefaultCreateOrUpdateBuilder<T> withToManyRelationship(String name,
			Class<? extends DataObject> relatedObjType, String keyAttribute, String relationshipKeyAttribute) {
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

		Mapper mapper = this.mapper != null ? this.mapper : mapperBuilder.build();
		Map<String, TargetPropertyWriter> writers = createTargetPropertyWriters();

		SourceMapper sourceMapper = new SourceMapper(mapper);
		TargetMatcher<T> targetMatcher = new TargetMatcher<>(type, mapper);
		CreateOrUpdateMerger<T> merger = new CreateOrUpdateMerger<>(type, mapper, writers);

		return new CreateOrUpdateSegmentProcessor<>(RowConverter.instance(), sourceMapper, targetMatcher, merger,
				stageListenersBuilder.getListeners(), loadListeners);
	}

	private Map<String, TargetPropertyWriter> createTargetPropertyWriters() {

		// TODO: this should be extracted in a singleton service - property
		// metadata should be compiled once and reused.

		EntityResolver entityResolver = targetCayenneService.entityResolver();
		ObjEntity entity = entityResolver.getObjEntity(type);
		ClassDescriptor descriptor = targetCayenneService.entityResolver().getClassDescriptor(entity.getName());

		final Map<String, TargetPropertyWriter> writers = new HashMap<>();

		// TODO: instead of providing mappings for all possible obj: and db:
		// invariants, should we normalize the source map instead?

		descriptor.visitProperties(new PropertyVisitor() {

			@Override
			public boolean visitAttribute(AttributeProperty property) {
				TargetPropertyWriter writer = new TargetAttributePropertyWriter(property);

				// store for both obj: and db: expressions
				writers.put(property.getName(), writer);
				writers.put(ASTDbPath.DB_PREFIX + property.getAttribute().getDbAttributeName(), writer);

				return true;
			}

			@Override
			public boolean visitToOne(ToOneProperty property) {
				TargetPropertyWriter writer = new TargetToOnePropertyWriter(property);

				// store for both obj: and db: expressions ..
				writers.put(property.getName(), writer);

				List<DbRelationship> dbRelationships = property.getRelationship().getDbRelationships();
				if (dbRelationships.size() > 1) {
					// TODO: support for flattened to-one relationships
					LOGGER.info("TODO: not mapping db: path for a flattened relationship: " + property.getName());
				} else {

					DbRelationship dbRelationship = dbRelationships.get(0);
					List<DbJoin> joins = dbRelationship.getJoins();

					if (joins.size() > 1) {
						// TODO: support for multi-key to-one relationships
						LOGGER.info("TODO: not mapping db: path for a multi-key relationship: " + property.getName());
					} else {
						writers.put(ASTDbPath.DB_PREFIX + joins.get(0).getSourceName(), writer);
					}
				}

				return true;
			}

			@Override
			public boolean visitToMany(ToManyProperty property) {
				writers.put(property.getName(), new TargetToManyPropertyWriter(property));
				return true;
			}
		});

		for (DbAttribute pk : entity.getDbEntity().getPrimaryKeys()) {
			String key = ASTDbPath.DB_PREFIX + pk.getName();
			if (!writers.containsKey(key)) {
				writers.put(key, new TargetPkPropertyWriter(pk));
			}
		}

		return writers;
	}
}
