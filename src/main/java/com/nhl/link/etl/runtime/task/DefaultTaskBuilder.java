package com.nhl.link.etl.runtime.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.exp.Property;
import org.apache.cayenne.map.ObjAttribute;
import org.apache.cayenne.map.ObjEntity;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowReader;
import com.nhl.link.etl.SyncToken;
import com.nhl.link.etl.batch.BatchProcessor;
import com.nhl.link.etl.batch.BatchRunner;
import com.nhl.link.etl.extract.Extractor;
import com.nhl.link.etl.extract.ExtractorParameters;
import com.nhl.link.etl.load.LoadListener;
import com.nhl.link.etl.load.cayenne.CayenneCreateOrUpdateLoader;
import com.nhl.link.etl.load.cayenne.CayenneCreateOrUpdateStrategy;
import com.nhl.link.etl.load.cayenne.CayenneCreateOrUpdateWithPKStrategy;
import com.nhl.link.etl.load.cayenne.DefaultCayenneCreateOrUpdateStrategy;
import com.nhl.link.etl.load.cayenne.RelationshipInfo;
import com.nhl.link.etl.load.cayenne.RelationshipType;
import com.nhl.link.etl.load.mapper.AttributeMapper;
import com.nhl.link.etl.load.mapper.IdMapper;
import com.nhl.link.etl.load.mapper.KeyAdapter;
import com.nhl.link.etl.load.mapper.Mapper;
import com.nhl.link.etl.load.mapper.MultiAttributeMapper;
import com.nhl.link.etl.load.mapper.SafeMapKeyMapper;
import com.nhl.link.etl.runtime.EtlRuntimeBuilder;
import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.load.mapper.IKeyAdapterFactory;
import com.nhl.link.etl.runtime.token.ITokenManager;
import com.nhl.link.etl.transform.RowToTargetMapConverter;

/**
 * A builder of an ETL task that matches source data with target data based on a
 * certain unique attribute on both sides.
 */
public class DefaultTaskBuilder<T extends DataObject> implements TaskBuilder<T> {

	private static final int DEFAULT_BATCH_SIZE = 500;

	private ITargetCayenneService targetCayenneService;
	private IExtractorService extractorService;
	private ITokenManager tokenManager;
	private IKeyAdapterFactory keyMapAdapterFactory;

	private Class<T> type;
	private String extractorName;
	private int batchSize;
	private List<RelationshipInfo> relationships;
	private List<LoadListener<T>> transformListeners;

	private Mapper<T> mapper;
	private boolean byId;
	private List<String> keyAttributes;

	@SuppressWarnings("unchecked")
	DefaultTaskBuilder(Class<T> type, ITargetCayenneService targetCayenneService, IExtractorService extractorService,
			ITokenManager tokenManager, IKeyAdapterFactory keyMapAdapterFactory) {

		this.extractorService = extractorService;
		this.batchSize = DEFAULT_BATCH_SIZE;
		this.type = type;
		this.targetCayenneService = targetCayenneService;
		this.tokenManager = tokenManager;
		this.relationships = new ArrayList<>();
		this.keyMapAdapterFactory = keyMapAdapterFactory;

		this.transformListeners = new ArrayList<>();

		// always add stats listener..
		transformListeners.add(StatsLoadListener.instance());
	}

	@Override
	public DefaultTaskBuilder<T> withExtractor(String extractorName) {
		this.extractorName = extractorName;
		return this;
	}

	@Override
	public DefaultTaskBuilder<T> matchBy(Mapper<T> mapper) {
		this.byId = false;
		this.mapper = mapper;
		this.keyAttributes = null;
		return this;
	}

	@Override
	public DefaultTaskBuilder<T> matchBy(String... keyAttributes) {
		this.byId = false;
		this.mapper = null;
		this.keyAttributes = Arrays.asList(keyAttributes);
		return this;
	}

	/**
	 * @since 1.1
	 */
	@Override
	public DefaultTaskBuilder<T> matchBy(Property<?>... matchAttributes) {

		// it will fail later on 'build'; TODO: should we do early argument
		// checking?
		if (matchAttributes == null) {
			return this;
		}
		String[] names = new String[matchAttributes.length];
		for (int i = 0; i < matchAttributes.length; i++) {
			names[i] = matchAttributes[i].getName();
		}

		return matchBy(names);
	}

	/**
	 * @deprecated since 1.1 use {@link #matchById(String)}.
	 */
	@Deprecated
	@Override
	public DefaultTaskBuilder<T> matchByPrimaryKey(String idProperty) {
		return matchById(idProperty);
	}

	/**
	 * @since 1.1
	 */
	@Override
	public DefaultTaskBuilder<T> matchById(String idProperty) {
		this.byId = true;
		this.mapper = null;
		this.keyAttributes = Collections.singletonList(idProperty);
		return this;
	}

	@Override
	public DefaultTaskBuilder<T> withBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	@Override
	public DefaultTaskBuilder<T> withToOneRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute) {
		this.relationships.add(new RelationshipInfo(name, keyAttribute, RelationshipType.TO_ONE, relatedObjType));
		return this;
	}

	@Override
	public DefaultTaskBuilder<T> withToManyRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute) {
		this.relationships.add(new RelationshipInfo(name, keyAttribute, RelationshipType.TO_MANY, relatedObjType));
		return this;
	}

	@Override
	public DefaultTaskBuilder<T> withToOneRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute, String relationshipKeyAttribute) {
		this.relationships.add(new RelationshipInfo(name, keyAttribute, RelationshipType.TO_ONE, relatedObjType,
				relationshipKeyAttribute));
		return this;
	}

	@Override
	public DefaultTaskBuilder<T> withToManyRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute, String relationshipKeyAttribute) {
		this.relationships.add(new RelationshipInfo(name, keyAttribute, RelationshipType.TO_MANY, relatedObjType,
				relationshipKeyAttribute));
		return this;
	}

	@Override
	public DefaultTaskBuilder<T> withListener(LoadListener<T> listener) {
		this.transformListeners.add(listener);
		return this;
	}

	private String getSingleMatchAttribute() {
		if (keyAttributes == null || keyAttributes.isEmpty()) {
			return null;
		}
		if (keyAttributes.size() > 1) {
			throw new IllegalStateException("Trying to get a single match attribute but multi key matching is set");
		}
		return keyAttributes.get(0);
	}

	private ObjAttribute getMatchAttribute() {

		ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);

		if (entity == null) {
			throw new IllegalStateException("Type " + type.getName() + " is not mapped in Cayenne");
		}

		if (byId) {
			return entity.getPrimaryKeys().iterator().next();
		} else {
			String matchAttribute = getSingleMatchAttribute();
			ObjAttribute a = entity.getAttribute(matchAttribute);
			if (a == null) {
				throw new IllegalStateException("Invalid attribute name " + matchAttribute + " for entity "
						+ entity.getName());
			}
			return a;
		}
	}

	private Mapper<T> createMatcher() {

		// not wrapping custom matcher, presuming the user knows what's he's
		// doing and his matcher generates proper keys
		if (this.mapper != null) {
			return this.mapper;
		}

		if (keyAttributes == null) {
			throw new IllegalStateException("'matcher' or 'matchAttribute' must be set");
		}

		Mapper<T> matcher;

		if (byId) {
			matcher = new IdMapper<>(pkAttribute(), getSingleMatchAttribute());
		} else if (keyAttributes.size() > 1) {
			matcher = new MultiAttributeMapper<>(keyAttributes);
		} else {
			matcher = new AttributeMapper<>(getSingleMatchAttribute());
		}

		KeyAdapter keyAdapter;

		// TODO: mapping keyMapAdapters by type doesn't take into account
		// composition and hierarchy of the keys ... need a different approach.
		// for now resorting to the hacks below
		if (keyAttributes.size() > 1) {
			keyAdapter = keyMapAdapterFactory.adapter(List.class);
		} else {
			ObjAttribute attribute = getMatchAttribute();
			keyAdapter = keyMapAdapterFactory.adapter(attribute.getJavaClass());
		}

		return new SafeMapKeyMapper<>(matcher, keyAdapter);
	}

	private String pkAttribute() {
		ObjEntity oe = targetCayenneService.entityResolver().getObjEntity(type);
		if (oe == null) {
			throw new EtlRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
		}

		Collection<String> pks = oe.getPrimaryKeyNames();
		if (pks.size() != 1) {
			throw new EtlRuntimeException("Only single-column PK is supported for now. Got " + pks.size()
					+ " for entity: " + oe.getName());
		}

		return pks.iterator().next();
	}

	@Override
	public EtlTask task() throws IllegalStateException {

		if (extractorName == null) {
			throw new IllegalStateException("Required 'extractorName' is not set");
		}

		final Mapper<T> matcher = createMatcher();

		return new EtlTask() {

			@Override
			public Execution run() {
				return run(SyncToken.nullToken(extractorName));
			}

			@Override
			public Execution run(SyncToken token) {

				try (Execution execution = new Execution(token);) {
					ObjectContext context = targetCayenneService.newContext();

					CayenneCreateOrUpdateStrategy<T> createOrUpdateStrategy;
					if (byId) {
						createOrUpdateStrategy = new CayenneCreateOrUpdateWithPKStrategy<>(relationships,
								getSingleMatchAttribute());
					} else {
						createOrUpdateStrategy = new DefaultCayenneCreateOrUpdateStrategy<>(relationships);
					}

					// processor is stateful and is not thread-safe, so creating
					// it every time...
					BatchProcessor<Map<String, Object>> processor = new CayenneCreateOrUpdateLoader<>(type, execution,
							matcher, createOrUpdateStrategy, transformListeners, context);

					ExtractorParameters extractorParams = new ExtractorParameters();
					SyncToken startToken = tokenManager.previousToken(token);
					extractorParams.add(EtlRuntimeBuilder.START_TOKEN_VAR, startToken.getValue());
					extractorParams.add(EtlRuntimeBuilder.END_TOKEN_VAR, token.getValue());

					try (RowReader data = getRowReader(execution, extractorName, extractorParams)) {
						BatchRunner.create(processor).withBatchSize(batchSize).run(data, RowToTargetMapConverter.instance());
						tokenManager.saveToken(token);
					}

					return execution;
				}
			}
		};
	}

	/**
	 * Returns a RowReader obtained from a named extractor and wrapped in a read
	 * stats counter.
	 */
	protected RowReader getRowReader(final Execution execution, String extractorName,
			ExtractorParameters extractorParams) {

		Extractor extractor = extractorService.getExtractor(extractorName);
		final RowReader reader = extractor.getReader(extractorParams);
		return new RowReader() {

			@Override
			public Iterator<Row> iterator() {
				final Iterator<Row> it = reader.iterator();

				return new Iterator<Row>() {
					@Override
					public boolean hasNext() {
						return it.hasNext();
					}

					@Override
					public void remove() {
						it.remove();
					}

					@Override
					public Row next() {
						execution.incrementExtracted(1);
						return it.next();
					}
				};
			}

			@Override
			public void close() {
				reader.close();
			}
		};
	}
}
