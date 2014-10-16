package com.nhl.link.etl.runtime.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.map.ObjAttribute;
import org.apache.cayenne.map.ObjEntity;

import com.nhl.link.etl.EtlTask;
import com.nhl.link.etl.Execution;
import com.nhl.link.etl.RowReader;
import com.nhl.link.etl.SyncToken;
import com.nhl.link.etl.batch.BatchRunner;
import com.nhl.link.etl.extract.ExtractorParameters;
import com.nhl.link.etl.extract.MapConverter;
import com.nhl.link.etl.keybuilder.IKeyBuilderFactory;
import com.nhl.link.etl.keybuilder.KeyBuilder;
import com.nhl.link.etl.runtime.EtlRuntimeBuilder;
import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.token.ITokenManager;
import com.nhl.link.etl.transform.AttributeMatcher;
import com.nhl.link.etl.transform.CayenneCreateOrUpdateStrategy;
import com.nhl.link.etl.transform.CayenneCreateOrUpdateTransformer;
import com.nhl.link.etl.transform.CayenneCreateOrUpdateWithPKStrategy;
import com.nhl.link.etl.transform.CayenneMatcher;
import com.nhl.link.etl.transform.DefaultCayenneCreateOrUpdateStrategy;
import com.nhl.link.etl.transform.MultiAttributeMatcher;
import com.nhl.link.etl.transform.PrimaryKeyMatcher;
import com.nhl.link.etl.transform.RelationshipInfo;
import com.nhl.link.etl.transform.RelationshipType;
import com.nhl.link.etl.transform.TransformListener;

/**
 * A builder of an ETL task that matches source data with target data based on a
 * certain unique attribute on both sides.
 */
public class MatchingTaskBuilder<T extends DataObject> extends BaseTaskBuilder {

	private static final int DEFAULT_BATCH_SIZE = 500;

	private ITargetCayenneService targetCayenneService;

	private ITokenManager tokenManager;
	private IKeyBuilderFactory keyBuilderFactory;

	private Class<T> type;
	private String extractorName;
	private int batchSize;
	private List<RelationshipInfo> relationships;
	private List<TransformListener<T>> transformListeners;

	private CayenneMatcher<T> matcher;
	private boolean pk;
	private List<String> matchAttributes;

	MatchingTaskBuilder(Class<T> type, ITargetCayenneService targetCayenneService, IExtractorService extractorService,
			ITokenManager tokenManager, IKeyBuilderFactory keyBuilderFactory) {

		super(extractorService);
		this.batchSize = DEFAULT_BATCH_SIZE;
		this.type = type;
		this.targetCayenneService = targetCayenneService;
		this.tokenManager = tokenManager;
		this.relationships = new ArrayList<>();
		this.transformListeners = new ArrayList<>();
		this.keyBuilderFactory = keyBuilderFactory;
	}

	public MatchingTaskBuilder<T> withExtractor(String extractorName) {
		this.extractorName = extractorName;
		return this;
	}

	public MatchingTaskBuilder<T> matchBy(CayenneMatcher<T> matcher) {
		this.pk = false;
		this.matcher = matcher;
		this.matchAttributes = null;
		return this;
	}

	public MatchingTaskBuilder<T> matchBy(String matchAttribute) {
		this.pk = false;
		this.matcher = null;
		this.matchAttributes = Collections.singletonList(matchAttribute);
		return this;
	}

	public MatchingTaskBuilder<T> matchByPrimaryKey(String matchPKAttribute) {
		this.pk = true;
		this.matcher = null;
		this.matchAttributes = Collections.singletonList(matchPKAttribute);
		return this;
	}

	public MatchingTaskBuilder<T> matchBy(String... matchAttributes) {
		this.pk = false;
		this.matcher = null;
		this.matchAttributes = Arrays.asList(matchAttributes);
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

	public MatchingTaskBuilder<T> withListener(TransformListener<T> listener) {
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

		if (pk) {
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

	public EtlTask task() throws IllegalStateException {

		if (extractorName == null) {
			throw new IllegalStateException("Required 'extractorName' is not set");
		}

		final CayenneMatcher<T> matcher;

		if (this.matcher != null) {
			matcher = this.matcher;
		} else if (matchAttributes != null) {
			KeyBuilder keyBuilder;
			if (matchAttributes.size() > 1) {
				keyBuilder = keyBuilderFactory.keyBuilder(List.class);
			} else {
				ObjAttribute attribute = getMatchAttribute();
				keyBuilder = keyBuilderFactory.keyBuilder(attribute.getJavaClass());
			}

			if (pk) {
				matcher = new PrimaryKeyMatcher<>(keyBuilder, getSingleMatchAttribute());
			} else if (matchAttributes.size() > 1) {
				matcher = new MultiAttributeMatcher<>(keyBuilder, matchAttributes);
			} else {
				matcher = new AttributeMatcher<>(keyBuilder, getSingleMatchAttribute());
			}
		} else {
			throw new IllegalStateException("'matcher' or 'matchAttribute' must be set");
		}

		return new EtlTask() {

			@Override
			public Execution run(SyncToken token) {

				try (Execution execution = new Execution(token);) {
					ObjectContext context = targetCayenneService.newContext();

					CayenneCreateOrUpdateStrategy<T> createOrUpdateStrategy;
					if (pk) {
						createOrUpdateStrategy = new CayenneCreateOrUpdateWithPKStrategy<>(relationships,
								getSingleMatchAttribute());
					} else {
						createOrUpdateStrategy = new DefaultCayenneCreateOrUpdateStrategy<>(relationships);
					}

					// processor is stateful and is not thread-safe, so creating
					// it every time...
					CayenneCreateOrUpdateTransformer<T> processor = new CayenneCreateOrUpdateTransformer<>(type,
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
