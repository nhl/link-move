package com.nhl.link.move.runtime;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.runtime.adapter.LinkEtlAdapter;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.cayenne.TargetCayenneService;
import com.nhl.link.move.runtime.cayenne.TargetConnectorFactory;
import com.nhl.link.move.runtime.connect.ConnectorService;
import com.nhl.link.move.runtime.connect.IConnectorFactory;
import com.nhl.link.move.runtime.connect.IConnectorService;
import com.nhl.link.move.runtime.extractor.ExtractorService;
import com.nhl.link.move.runtime.extractor.IExtractorFactory;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.extractor.model.ClasspathExtractorModelLoader;
import com.nhl.link.move.runtime.extractor.model.ExtractorModelService;
import com.nhl.link.move.runtime.extractor.model.FileExtractorModelLoader;
import com.nhl.link.move.runtime.extractor.model.IExtractorModelLoader;
import com.nhl.link.move.runtime.extractor.model.IExtractorModelService;
import com.nhl.link.move.runtime.jdbc.BigIntNormalizer;
import com.nhl.link.move.runtime.jdbc.BooleanNormalizer;
import com.nhl.link.move.runtime.jdbc.DecimalNormalizer;
import com.nhl.link.move.runtime.jdbc.IntegerNormalizer;
import com.nhl.link.move.runtime.jdbc.JdbcConnector;
import com.nhl.link.move.runtime.jdbc.JdbcExtractorFactory;
import com.nhl.link.move.runtime.jdbc.JdbcNormalizer;
import com.nhl.link.move.runtime.jdbc.LocalDateNormalizer;
import com.nhl.link.move.runtime.jdbc.LocalDateTimeNormalizer;
import com.nhl.link.move.runtime.jdbc.LocalTimeNormalizer;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.key.KeyAdapterFactory;
import com.nhl.link.move.runtime.path.IPathNormalizer;
import com.nhl.link.move.runtime.path.PathNormalizer;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.TaskService;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.runtime.token.InMemoryTokenManager;
import com.nhl.link.move.writer.ITargetPropertyWriterService;
import com.nhl.link.move.writer.TargetPropertyWriterService;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.dba.TypesMapping;
import org.apache.cayenne.di.Binder;
import org.apache.cayenne.di.DIBootstrap;
import org.apache.cayenne.di.Injector;
import org.apache.cayenne.di.Key;
import org.apache.cayenne.di.MapBuilder;
import org.apache.cayenne.di.Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;

/**
 * A builder class that helps to assemble working LinkEtl stack.
 */
public class LmRuntimeBuilder {

	private static final Logger LOGGER = LoggerFactory.getLogger(LmRuntimeBuilder.class);

	public static final String CONNECTORS_MAP = "com.nhl.link.move.connectors";
	public static final String CONNECTOR_FACTORIES_MAP = "com.nhl.link.move.connector.factories";
	public static final String EXTRACTOR_FACTORIES_MAP = "com.nhl.link.move.extractor.factories";

	public static final String JDBC_NORMALIZERS_MAP = "com.nhl.link.move.jdbc.normalizers";

	/**
	 * A DI property that defines the root directory to resolve locations of
	 * extractor config files.
	 * 
	 * @since 1.4
	 */
	public static final String FILE_EXTRACTOR_MODEL_ROOT_DIR = "com.nhl.link.move.extrator.root.dir";

	public static final String START_TOKEN_VAR = "startToken";
	public static final String END_TOKEN_VAR = "endToken";

	private Map<String, Connector> connectors;
	private Map<String, IConnectorFactory<? extends Connector>> connectorFactories;
	private Map<String, Class<? extends IConnectorFactory<? extends Connector>>> connectorFactoryTypes;

	private Map<String, IExtractorFactory<?>> extractorFactories;
	private Map<String, Class<? extends IExtractorFactory<?>>> extractorFactoryTypes;

	private Map<String, JdbcNormalizer<?>> jdbcNormalizers;

	private IExtractorModelLoader extractorModelLoader;
	private File extractorModelsRoot;

	private ITokenManager tokenManager;
	private ServerRuntime targetRuntime;
	private Collection<LinkEtlAdapter> adapters;

	public LmRuntimeBuilder() {
		this.connectors = new HashMap<>();
		this.connectorFactories = new HashMap<>();
		this.connectorFactoryTypes = new HashMap<>();
		this.extractorFactories = new HashMap<>();
		this.extractorFactoryTypes = new HashMap<>();
		this.jdbcNormalizers = new HashMap<>();
		this.adapters = new ArrayList<>();

		// default normalizers
		jdbcNormalizers.put(Long.class.getName(), new BigIntNormalizer());
		jdbcNormalizers.put(Integer.class.getName(), new IntegerNormalizer());
		jdbcNormalizers.put(BigDecimal.class.getName(), new DecimalNormalizer());
		jdbcNormalizers.put(Boolean.class.getName(), new BooleanNormalizer());
		jdbcNormalizers.put(LocalDate.class.getName(), new LocalDateNormalizer());
		jdbcNormalizers.put(LocalTime.class.getName(), new LocalTimeNormalizer());
		jdbcNormalizers.put(LocalDateTime.class.getName(), new LocalDateTimeNormalizer());
	}

	/**
	 * Adds an adapter that can override existing DI services or add new ones.
	 *
	 * @since 1.1
	 */
	public LmRuntimeBuilder adapter(LinkEtlAdapter adapter) {
		this.adapters.add(adapter);
		return this;
	}

	/**
	 * Sets a target Cayenne runtime for this ETL stack.
	 */
	public LmRuntimeBuilder withTargetRuntime(ServerRuntime targetRuntime) {
		this.targetRuntime = targetRuntime;
		return this;
	}

	public LmRuntimeBuilder withTokenManager(ITokenManager tokenManager) {
		this.tokenManager = tokenManager;
		return this;
	}

	public LmRuntimeBuilder withConnector(String id, Connector connector) {
		connectors.put(id, connector);
		return this;
	}

	public <C extends Connector> LmRuntimeBuilder withConnectorFactory(Class<C> connectorType,
			IConnectorFactory<C> factory) {
		connectorFactories.put(connectorType.getName(), factory);
		return this;
	}

	public <C extends Connector> LmRuntimeBuilder withConnectorFactory(Class<C> connectorType,
			Class<? extends IConnectorFactory<C>> factoryType) {
		connectorFactoryTypes.put(connectorType.getName(), factoryType);
		return this;
	}

	/**
	 * Ensures source JDBC connectors are created for each one of the target
	 * DataSources. Connector ID will be equal to the DataNode name of the
	 * target.
	 * <p>
	 * This configuration conveniently reuses already pre-configured target
	 * DataSources. It may be useful in cases when the data is transferred
	 * between the tables on the same DB server, or if connectors are known in
	 * advance and can be modeled in Cayenne as DataNodes.
	 *
	 * @since 1.1
	 */
	public LmRuntimeBuilder withConnectorFromTarget() {
		connectorFactoryTypes.put(JdbcConnector.class.getName(), TargetConnectorFactory.class);
		return this;
	}

	/**
	 * Adds an extra factory to the map of extractor factories. Note that
	 * {@link JdbcExtractorFactory} is loaded by default and does not have to be
	 * configured explicitly.
	 */
	public LmRuntimeBuilder withExtractorFactory(String extractorType, Class<? extends IExtractorFactory<?>> factoryType) {
		extractorFactoryTypes.put(extractorType, factoryType);
		return this;
	}

	/**
	 * Adds an extra factory to the map of extractor factories. Note that
	 * {@link JdbcExtractorFactory} is loaded by default and does not have to be
	 * configured explicitly.
	 */
	public LmRuntimeBuilder withExtractorFactory(String extractorType, IExtractorFactory<?> factory) {
		extractorFactories.put(extractorType, factory);
		return this;
	}

	@Deprecated
	public LmRuntimeBuilder withJdbcNormalizer(int jdbcType, JdbcNormalizer<?> normalizer) {
		String javaType = TypesMapping.getJavaBySqlType(jdbcType);
		if (javaType == null) {
			throw new LmRuntimeException("Can't map JDBC type to Java type: " + jdbcType);
		}
		jdbcNormalizers.put(javaType, normalizer);
		return this;
	}

	/**
	 * @since 1.7
     */
	public LmRuntimeBuilder withJdbcNormalizer(Class<?> javaType, JdbcNormalizer<?> normalizer) {
		jdbcNormalizers.put(javaType.getName(), normalizer);
		return this;
	}

	/**
	 * @since 1.4
	 */
	public LmRuntimeBuilder extractorModelLoader(IExtractorModelLoader extractorModelLoader) {
		this.extractorModelLoader = extractorModelLoader;
		this.extractorModelsRoot = null;
		return this;
	}

	/**
	 * @since 1.4
	 */
	public LmRuntimeBuilder extractorModelsRoot(File rootDir) {
		this.extractorModelLoader = null;
		this.extractorModelsRoot = rootDir;
		return this;
	}

	/**
	 * @since 1.4
	 */
	public LmRuntimeBuilder extractorModelsRoot(String rootDirPath) {
		this.extractorModelLoader = null;
		this.extractorModelsRoot = new File(rootDirPath);
		return this;
	}

	public LmRuntime build() throws IllegalStateException {

		if (targetRuntime == null) {
			throw new IllegalStateException("Required Cayenne 'targetRuntime' is not set");
		}

		if (tokenManager == null) {
			tokenManager = new InMemoryTokenManager();
		}

		Module etlModule = new Module() {

			@SuppressWarnings({ "unchecked", "rawtypes" })
			@Override
			public void configure(Binder binder) {

				bindModelLoader(binder);

				binder.<Connector> bindMap(LmRuntimeBuilder.CONNECTORS_MAP).putAll(connectors);

				MapBuilder<IConnectorFactory<? extends Connector>> connectorFactories = binder
						.<IConnectorFactory<? extends Connector>> bindMap(LmRuntimeBuilder.CONNECTOR_FACTORIES_MAP);

				connectorFactories.putAll(LmRuntimeBuilder.this.connectorFactories);

				for (Entry<String, Class<? extends IConnectorFactory<? extends Connector>>> e : connectorFactoryTypes
						.entrySet()) {

					// a bit ugly - need to bind all factory types explicitly
					// before placing then in a map .. also must drop
					// parameterization to be able to bind with non-specific
					// boundaries (<? extends ...>)
					Class efType = e.getValue();
					binder.bind(efType).to(efType);

					connectorFactories.put(e.getKey(), e.getValue());
				}

				MapBuilder<IExtractorFactory<?>> extractorFactories = binder.bindMap(LmRuntimeBuilder.EXTRACTOR_FACTORIES_MAP);

				ServiceLoader<IExtractorFactory> extractorFactoryServiceLoader = ServiceLoader.load(IExtractorFactory.class);
				for (IExtractorFactory extractorFactory : extractorFactoryServiceLoader) {
					extractorFactories.put(extractorFactory.getExtractorType(), extractorFactory);
				}

				extractorFactories.putAll(LmRuntimeBuilder.this.extractorFactories);

				for (Entry<String, Class<? extends IExtractorFactory<?>>> e : extractorFactoryTypes.entrySet()) {

					// a bit ugly - need to bind all factory types explicitly
					// before placing then in a map .. also must drop
					// parameterization to be able to bind with non-specific
					// boundaries (<? extends ...>)
					Class efType = e.getValue();
					binder.bind(efType).to(efType);

					extractorFactories.put(e.getKey(), e.getValue());
				}

				MapBuilder<JdbcNormalizer<?>> jdbcNormalizers = binder.bindMap(LmRuntimeBuilder.JDBC_NORMALIZERS_MAP);
				jdbcNormalizers.putAll(LmRuntimeBuilder.this.jdbcNormalizers);

				// Binding CayenneService for the *target*... Note that binding
				// ServerRuntime directly would result in undesired shutdown
				// when the ETL module is shutdown.
				binder.bind(ITargetCayenneService.class).toInstance(new TargetCayenneService(targetRuntime));

				binder.bind(IExtractorService.class).to(ExtractorService.class);
				binder.bind(IConnectorService.class).to(ConnectorService.class);
				binder.bind(ITaskService.class).to(TaskService.class);
				binder.bind(ITokenManager.class).toInstance(tokenManager);
				binder.bind(IKeyAdapterFactory.class).to(KeyAdapterFactory.class);
				binder.bind(IPathNormalizer.class).to(PathNormalizer.class);
				binder.bind(ITargetPropertyWriterService.class).to(TargetPropertyWriterService.class);
				binder.bind(IExtractorModelService.class).to(ExtractorModelService.class);

				// apply adapter-contributed bindings
				for (LinkEtlAdapter a : adapters) {
					a.contributeToRuntime(binder);
				}
			}
		};

		final Injector injector = DIBootstrap.createInjector(etlModule);

		return new LmRuntime() {

			@Override
			public <T> T service(Class<T> serviceType) {
				return injector.getInstance(serviceType);
			}

			@Override
			public void shutdown() {
				injector.shutdown();
			}
		};
	}

	void bindModelLoader(Binder binder) {
		if (extractorModelLoader != null) {
			binder.bind(IExtractorModelLoader.class).toInstance(extractorModelLoader);
		} else if (extractorModelsRoot != null) {

			if (!extractorModelsRoot.isDirectory()) {
				LOGGER.warn("Extractor models root is not a valid directory: " + extractorModelsRoot);
			}

			binder.bind(IExtractorModelLoader.class).to(FileExtractorModelLoader.class);
			binder.bind(Key.get(File.class, FILE_EXTRACTOR_MODEL_ROOT_DIR)).toInstance(extractorModelsRoot);
		} else {
			binder.bind(IExtractorModelLoader.class).to(ClasspathExtractorModelLoader.class);
		}
	}

}
