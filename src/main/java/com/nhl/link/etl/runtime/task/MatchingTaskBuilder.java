package com.nhl.link.etl.runtime.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.exp.Property;
import org.apache.cayenne.map.ObjAttribute;
import org.apache.cayenne.map.ObjEntity;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.RowReader;
import com.nhl.link.etl.SyncToken;
import com.nhl.link.etl.batch.BatchRunner;
import com.nhl.link.etl.extract.ExtractorParameters;
import com.nhl.link.etl.extract.MapConverter;
import com.nhl.link.etl.load.RelationshipInfo;
import com.nhl.link.etl.load.RelationshipType;
import com.nhl.link.etl.load.LoadListener;
import com.nhl.link.etl.load.cayenne.CayenneCreateOrUpdateLoader;
import com.nhl.link.etl.load.cayenne.CayenneCreateOrUpdateStrategy;
import com.nhl.link.etl.load.cayenne.CayenneCreateOrUpdateWithPKStrategy;
import com.nhl.link.etl.load.cayenne.DefaultCayenneCreateOrUpdateStrategy;
import com.nhl.link.etl.load.matcher.AttributeMatcher;
import com.nhl.link.etl.load.matcher.IdMatcher;
import com.nhl.link.etl.load.matcher.KeyAdapter;
import com.nhl.link.etl.load.matcher.Matcher;
import com.nhl.link.etl.load.matcher.MultiAttributeMatcher;
import com.nhl.link.etl.load.matcher.SafeMapKeyMatcher;
import com.nhl.link.etl.runtime.EtlRuntimeBuilder;
import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.matcher.IKeyAdapterFactory;
import com.nhl.link.etl.runtime.token.ITokenManager;

/**
 * A builder of an ETL task that matches source data with target data based on a
 * certain unique attribute on both sides.
 */
public class MatchingTaskBuilder<T extends DataObject> extends BaseTaskBuilder {

	private static final int DEFAULT_BATCH_SIZE = 500;

	private ITargetCayenneService targetCayenneService;

	private ITokenManager tokenManager;
	private IKeyAdapterFactory keyMapAdapterFactory;

	private Class<T> type;
	private String extractorName;
	private int batchSize;
	private List<RelationshipInfo> relationships;
	private List<LoadListener<T>> transformListeners;

	private Matcher<T> matcher;
	private boolean byId;
	private List<String> matchAttributes;

	MatchingTaskBuilder(Class<T> type, ITargetCayenneService targetCayenneService, IExtractorService extractorService,
			ITokenManager tokenManager, IKeyAdapterFactory keyMapAdapterFactory) {

		super(extractorService);
		this.batchSize = DEFAULT_BATCH_SIZE;
		this.type = type;
		this.targetCayenneService = targetCayenneService;
		this.tokenManager = tokenManager;
		this.relationships = new ArrayList<>();
		this.transformListeners = new ArrayList<>();
		this.keyMapAdapterFactory = keyMapAdapterFactory;
	}

	public MatchingTaskBuilder<T> withExtractor(String extractorName) {
		this.extractorName = extractorName;
		return this;
	}

	public MatchingTaskBuilder<T> matchBy(Matcher<T> matcher) {
		this.byId = false;
		this.matcher = matcher;
		this.matchAttributes = null;
		return this;
	}

	public MatchingTaskBuilder<T> matchBy(String... matchAttributes) {
		this.byId = false;
		this.matcher = null;
		this.matchAttributes = Arrays.asList(matchAttributes);
		return this;
	}

	/**
	 * @since 1.1
	 */
	public MatchingTaskBuilder<T> matchBy(Property<?> matchAttribute) {
		return matchBy(matchAttribute.getName());
	}

	/**
	 * @deprecated since 1.1 use {@link #matchById(String)}.
	 */
	@Deprecated
	public MatchingTaskBuilder<T> matchByPrimaryKey(String idProperty) {
		return matchById(idProperty);
	}

	/**
	 * @since 1.1
	 */
	public MatchingTaskBuilder<T> matchById(String idProperty) {
		this.byId = true;
		this.matcher = null;
		this.matchAttributes = Collections.singletonList(idProperty);
		return this;
	}

	public MatchingTaskBuilder<T> withBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	public MatchingTaskBuilder<T> withToOneRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute) {
		this.relationships.add(new RelationshipInfo(name, keyAttribute, RelationshipType.TO_ONE, relatedObjType));
		return this;
	}

	public MatchingTaskBuilder<T> withToManyRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute) {
		this.relationships.add(new RelationshipInfo(name, keyAttribute, RelationshipType.TO_MANY, relatedObjType));
		return this;
	}

	public MatchingTaskBuilder<T> withToOneRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute, String relationshipKeyAttribute) {
		this.relationships.add(new RelationshipInfo(name, keyAttribute, RelationshipType.TO_ONE, relatedObjType,
				relationshipKeyAttribute));
		return this;
	}

	public MatchingTaskBuilder<T> withToManyRelationship(String name, Class<? extends DataObject> relatedObjType,
			String keyAttribute, String relationshipKeyAttribute) {
		this.relationships.add(new RelationshipInfo(name, keyAttribute, RelationshipType.TO_MANY, relatedObjType,
				relationshipKeyAttribute));
		return this;
	}

	public MatchingTaskBuilder<T> withListener(LoadListener<T> listener) {
		this.transformListeners.add(listener);
		return this;
	}

	private String getSingleMatchAttribute() {
		if (matchAttributes == null || matchAttributes.isEmpty()) {
			return null;
		}
		if (matchAttributes.size() > 1) {
			throw new IllegalStateException("Trying to get a single match attribute but multi key matching is set");
		}
		return matchAttributes.get(0);
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

	private Matcher<T> createMatcher() {

		// not wrapping custom matcher, presuming the user knows what's he's
		// doing and his matcher generates proper keys
		if (this.matcher != null) {
			return this.matcher;
		}

		if (matchAttributes == null) {
			throw new IllegalStateException("'matcher' or 'matchAttribute' must be set");
		}

		Matcher<T> matcher;

		if (byId) {
			matcher = new IdMatcher<>(pkAttribute(), getSingleMatchAttribute());
		} else if (matchAttributes.size() > 1) {
			matcher = new MultiAttributeMatcher<>(matchAttributes);
		} else {
			matcher = new AttributeMatcher<>(getSingleMatchAttribute());
		}

		KeyAdapter keyAdapter;

		// TODO: mapping keyMapAdapters by type doesn't take into account
		// composition and hierarchy of the keys ... need a different approach.
		// for now resorting to the hacks below
		if (matchAttributes.size() > 1) {
			keyAdapter = keyMapAdapterFactory.adapter(List.class);
		} else {
			ObjAttribute attribute = getMatchAttribute();
			keyAdapter = keyMapAdapterFactory.adapter(attribute.getJavaClass());
		}

		return new SafeMapKeyMatcher<>(matcher, keyAdapter);
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

	public EtlTask task() throws IllegalStateException {

		if (extractorName == null) {
			throw new IllegalStateException("Required 'extractorName' is not set");
		}

		final Matcher<T> matcher = createMatcher();

		return new EtlTask() {

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
					CayenneCreateOrUpdateLoader<T> processor = new CayenneCreateOrUpdateLoader<>(type,
							execution, matcher, createOrUpdateStrategy, transformListeners, context);

					ExtractorParameters extractorParams = new ExtractorParameters();
					SyncToken startToken = tokenManager.previousToken(token);
					extractorParams.add(EtlRuntimeBuilder.START_TOKEN_VAR, startToken.getValue());
					extractorParams.add(EtlRuntimeBuilder.END_TOKEN_VAR, token.getValue());

					try (RowReader data = getRowReader(execution, extractorName, extractorParams)) {
						BatchRunner.create(processor).withBatchSize(batchSize).run(data, MapConverter.instance());
						tokenManager.saveToken(token);
					}

					return execution;
				}
			}
		};
	}
}
