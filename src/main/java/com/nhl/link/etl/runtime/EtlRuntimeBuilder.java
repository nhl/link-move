package com.nhl.link.etl.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.nhl.link.etl.runtime.file.csv.CsvExtractorFactory;
import com.nhl.link.etl.runtime.xml.XmlExtractorFactory;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.di.Binder;
import org.apache.cayenne.di.DIBootstrap;
import org.apache.cayenne.di.Injector;
import org.apache.cayenne.di.MapBuilder;
import org.apache.cayenne.di.Module;

import com.nhl.link.etl.connect.Connector;
import com.nhl.link.etl.connect.IConnectorFactory;
import com.nhl.link.etl.runtime.adapter.LinkEtlAdapter;
import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.cayenne.TargetCayenneService;
import com.nhl.link.etl.runtime.cayenne.TargetConnectorFactory;
import com.nhl.link.etl.runtime.connect.ConnectorService;
import com.nhl.link.etl.runtime.connect.IConnectorService;
import com.nhl.link.etl.runtime.extract.ClasspathExtractorConfigLoader;
import com.nhl.link.etl.runtime.extract.ExtractorService;
import com.nhl.link.etl.runtime.extract.IExtractorConfigLoader;
import com.nhl.link.etl.runtime.extract.IExtractorFactory;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.jdbc.JdbcConnector;
import com.nhl.link.etl.runtime.jdbc.JdbcExtractorFactory;
import com.nhl.link.etl.runtime.key.IKeyAdapterFactory;
import com.nhl.link.etl.runtime.key.KeyAdapterFactory;
import com.nhl.link.etl.runtime.task.ITaskService;
import com.nhl.link.etl.runtime.task.TaskService;
import com.nhl.link.etl.runtime.token.ITokenManager;
import com.nhl.link.etl.runtime.token.InMemoryTokenManager;

/**
 * A builder class that helps to assemble working LinkEtl stack.
 */
public class EtlRuntimeBuilder {

	public static final String CONNECTORS_MAP = "com.nhl.link.etl.connectors";
	public static final String CONNECTOR_FACTORIES_MAP = "com.nhl.link.etl.connector.factories";
	public static final String EXTRACTOR_FACTORIES_MAP = "com.nhl.link.etl.extractor.factories";

	public static final String JDBC_EXTRACTOR_TYPE = "jdbc";
	public static final String CSV_EXTRACTOR_TYPE = "csv";
	public static final String XML_EXTRACTOR_TYPE = "xml";

	public static final String START_TOKEN_VAR = "startToken";
	public static final String END_TOKEN_VAR = "endToken";

	private Map<String, Connector> connectors;
	private Map<String, IConnectorFactory<? extends Connector>> connectorFactories;
	private Map<String, Class<? extends IConnectorFactory<? extends Connector>>> connectorFactoryTypes;

	private Map<String, IExtractorFactory> extractorFactories;
	private Map<String, Class<? extends IExtractorFactory>> extractorFactoryTypes;

	private IExtractorConfigLoader extractorConfigLoader;
	private ITokenManager tokenManager;
	private ServerRuntime targetRuntime;
	private Collection<LinkEtlAdapter> adapters;

	public EtlRuntimeBuilder() {
		this.connectors = new HashMap<>();
		this.connectorFactories = new HashMap<>();
		this.connectorFactoryTypes = new HashMap<>();
		this.extractorFactories = new HashMap<>();
		this.extractorFactoryTypes = new HashMap<>();
		this.adapters = new ArrayList<>();

		// default extractors
		extractorFactoryTypes.put(JDBC_EXTRACTOR_TYPE, JdbcExtractorFactory.class);
		extractorFactoryTypes.put(CSV_EXTRACTOR_TYPE, CsvExtractorFactory.class);
		extractorFactoryTypes.put(XML_EXTRACTOR_TYPE, XmlExtractorFactory.class);
	}

	/**
	 * Adds an adapter that can override existing DI services or add new ones.
	 *
	 * @since 1.1
	 */
	public EtlRuntimeBuilder adapter(LinkEtlAdapter adapter) {
		this.adapters.add(adapter);
		return this;
	}

	/**
	 * Sets a target Cayenne runtime for this ETL stack.
	 */
	public EtlRuntimeBuilder withTargetRuntime(ServerRuntime targetRuntime) {
		this.targetRuntime = targetRuntime;
		return this;
	}

	public EtlRuntimeBuilder withTokenManager(ITokenManager tokenManager) {
		this.tokenManager = tokenManager;
		return this;
	}

	public EtlRuntimeBuilder withConnector(String id, Connector connector) {
		connectors.put(id, connector);
		return this;
	}

	public <C extends Connector> EtlRuntimeBuilder withConnectorFactory(Class<C> connectorType,
			IConnectorFactory<C> factory) {
		connectorFactories.put(connectorType.getName(), factory);
		return this;
	}

	public <C extends Connector> EtlRuntimeBuilder withConnectorFactory(Class<C> connectorType,
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
	 * between the tables on the sane DB server, or if connectors are known
	 * upfront and hence can be modeled in Cayenne as DataNodes.
	 *
	 * @since 1.1
	 */
	public EtlRuntimeBuilder withConnectorFromTarget() {
		connectorFactoryTypes.put(JdbcConnector.class.getName(), TargetConnectorFactory.class);
		return this;
	}

	/**
	 * Adds an extra factory to the map of extractor factories. Note that
	 * {@link JdbcExtractorFactory} is loaded by default and does not have to be
	 * configured explicitly.
	 */
	public EtlRuntimeBuilder withExtractorFactory(String extractorType, Class<? extends IExtractorFactory> factoryType) {
		extractorFactoryTypes.put(extractorType, factoryType);
		return this;
	}

	/**
	 * Adds an extra factory to the map of extractor factories. Note that
	 * {@link JdbcExtractorFactory} is loaded by default and does not have to be
	 * configured explicitly.
	 */
	public EtlRuntimeBuilder withExtractorFactory(String extractorType, IExtractorFactory factory) {
		extractorFactories.put(extractorType, factory);
		return this;
	}

	public EtlRuntimeBuilder withExtractorConfigLoader(IExtractorConfigLoader extractorConfigLoader) {
		this.extractorConfigLoader = extractorConfigLoader;
		return this;
	}

	public EtlRuntime build() throws IllegalStateException {

		if (targetRuntime == null) {
			throw new IllegalStateException("Required Cayenne 'targetRuntime' is not set");
		}

		if (extractorConfigLoader == null) {
			extractorConfigLoader = new ClasspathExtractorConfigLoader();
		}

		if (tokenManager == null) {
			tokenManager = new InMemoryTokenManager();
		}

		Module etlModule = new Module() {

			@SuppressWarnings({ "unchecked", "rawtypes" })
			@Override
			public void configure(Binder binder) {

				binder.<Connector> bindMap(EtlRuntimeBuilder.CONNECTORS_MAP).putAll(connectors);

				MapBuilder<IConnectorFactory<? extends Connector>> connectorFactories = binder
						.<IConnectorFactory<? extends Connector>> bindMap(EtlRuntimeBuilder.CONNECTOR_FACTORIES_MAP);

				connectorFactories.putAll(EtlRuntimeBuilder.this.connectorFactories);

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

				MapBuilder<IExtractorFactory> extractorFactories = binder
						.<IExtractorFactory> bindMap(EtlRuntimeBuilder.EXTRACTOR_FACTORIES_MAP);
				extractorFactories.putAll(EtlRuntimeBuilder.this.extractorFactories);

				for (Entry<String, Class<? extends IExtractorFactory>> e : extractorFactoryTypes.entrySet()) {

					// a bit ugly - need to bind all factory types explicitly
					// before placing then in a map .. also must drop
					// parameterization to be able to bind with non-specific
					// boundaries (<? extends ...>)
					Class efType = e.getValue();
					binder.bind(efType).to(efType);

					extractorFactories.put(e.getKey(), e.getValue());
				}

				// Binding CayenneService for the *target*... Note that binding
				// ServerRuntime directly would result in undesired shutdown
				// when the ETL module is shutdown.
				binder.bind(ITargetCayenneService.class).toInstance(new TargetCayenneService(targetRuntime));

				binder.bind(IExtractorConfigLoader.class).toInstance(extractorConfigLoader);
				binder.bind(IExtractorService.class).to(ExtractorService.class);
				binder.bind(IConnectorService.class).to(ConnectorService.class);
				binder.bind(ITaskService.class).to(TaskService.class);
				binder.bind(ITokenManager.class).toInstance(tokenManager);
				binder.bind(IKeyAdapterFactory.class).to(KeyAdapterFactory.class);

				// apply adapter-contributed bindings
				for (LinkEtlAdapter a : adapters) {
					a.contributeToRuntime(binder);
				}
			}
		};

		final Injector injector = DIBootstrap.createInjector(etlModule);

		return new EtlRuntime() {

			@Override
			public ITaskService getTaskService() {
				return injector.getInstance(ITaskService.class);
			}

			@Override
			public void shutdown() {
				injector.shutdown();
			}
		};
	}

}
