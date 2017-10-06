package com.nhl.link.move.runtime;

import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.extractor.parser.ExtractorModelParser;
import com.nhl.link.move.extractor.parser.IExtractorModelParser;
import com.nhl.link.move.resource.ClasspathResourceResolver;
import com.nhl.link.move.resource.FileResourceResolver;
import com.nhl.link.move.resource.ResourceResolver;
import com.nhl.link.move.resource.URLResourceResolver;
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
import com.nhl.link.move.runtime.extractor.model.ExtractorModelService;
import com.nhl.link.move.runtime.extractor.model.IExtractorModelService;
import com.nhl.link.move.runtime.jdbc.JdbcConnector;
import com.nhl.link.move.runtime.jdbc.JdbcExtractorFactory;
import com.nhl.link.move.runtime.jdbc.JdbcNormalizer;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.key.KeyAdapterFactory;
import com.nhl.link.move.runtime.path.IPathNormalizer;
import com.nhl.link.move.runtime.path.PathNormalizer;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.TaskService;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.runtime.token.InMemoryTokenManager;
import com.nhl.link.move.valueconverter.BigDecimalConverter;
import com.nhl.link.move.valueconverter.BooleanConverter;
import com.nhl.link.move.valueconverter.IntegerConverter;
import com.nhl.link.move.valueconverter.LocalDateConverter;
import com.nhl.link.move.valueconverter.LocalDateTimeConverter;
import com.nhl.link.move.valueconverter.LocalTimeConverter;
import com.nhl.link.move.valueconverter.LongConverter;
import com.nhl.link.move.valueconverter.StringConverter;
import com.nhl.link.move.valueconverter.ValueConverter;
import com.nhl.link.move.valueconverter.ValueConverterFactory;
import com.nhl.link.move.writer.ITargetPropertyWriterService;
import com.nhl.link.move.writer.TargetPropertyWriterService;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.dba.TypesMapping;
import org.apache.cayenne.di.DIBootstrap;
import org.apache.cayenne.di.Injector;
import org.apache.cayenne.di.MapBuilder;
import org.apache.cayenne.di.Module;
import org.apache.cayenne.map.DbAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.function.Supplier;

/**
 * A builder class that helps to assemble working LinkEtl stack.
 */
public class LmRuntimeBuilder {

    /**
     * A DI property that defines the root directory to resolve locations of
     * extractor config files.
     *
     * @since 1.4
     * @deprecated since 2.4 unused
     */
    public static final String FILE_EXTRACTOR_MODEL_ROOT_DIR = "com.nhl.link.move.extrator.root.dir";

    public static final String START_TOKEN_VAR = "startToken";
    public static final String END_TOKEN_VAR = "endToken";
    private static final Logger LOGGER = LoggerFactory.getLogger(LmRuntimeBuilder.class);
    private Map<String, Connector> connectors;
    private Map<String, IConnectorFactory> connectorFactories;
    private Map<String, Class<? extends IConnectorFactory<? extends Connector>>> connectorFactoryTypes;

    private Map<String, IExtractorFactory> extractorFactories;
    private Map<String, Class<? extends IExtractorFactory<?>>> extractorFactoryTypes;

    private Map<String, ValueConverter> valueConverters;
    private Supplier<ResourceResolver> extractorResolverFactory;

    private ITokenManager tokenManager;
    private ServerRuntime targetRuntime;
    private Collection<LinkEtlAdapter> adapters;

    public LmRuntimeBuilder() {
        this.connectors = new HashMap<>();
        this.connectorFactories = new HashMap<>();
        this.connectorFactoryTypes = new HashMap<>();
        this.extractorFactories = new HashMap<>();
        this.extractorFactoryTypes = new HashMap<>();
        this.valueConverters = new HashMap<>();
        this.adapters = new ArrayList<>();
        this.extractorResolverFactory = () -> new ClasspathResourceResolver();

        // default normalizers
        valueConverters.put(Boolean.class.getName(), BooleanConverter.getConverter());
        valueConverters.put(Boolean.TYPE.getName(), BooleanConverter.getConverter());
        valueConverters.put(Long.class.getName(), LongConverter.getConverter());
        valueConverters.put(Long.TYPE.getName(), LongConverter.getConverter());
        valueConverters.put(Integer.class.getName(), IntegerConverter.getConverter());
        valueConverters.put(Integer.TYPE.getName(), IntegerConverter.getConverter());
        valueConverters.put(BigDecimal.class.getName(), new BigDecimalConverter());
        valueConverters.put(LocalDate.class.getName(), new LocalDateConverter());
        valueConverters.put(LocalTime.class.getName(), new LocalTimeConverter());
        valueConverters.put(LocalDateTime.class.getName(), new LocalDateTimeConverter());
        valueConverters.put(String.class.getName(), new StringConverter());
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

    /**
     * @since 2.4
     */
    public LmRuntimeBuilder valueConverter(Class<?> javaType, ValueConverter converter) {
        valueConverters.put(javaType.getName(), converter);
        return this;
    }

    /**
     * @since 1.7
     * @deprecated since 2.4 in favor of {@link #valueConverter(Class, ValueConverter)}.
     */
    @Deprecated
    public LmRuntimeBuilder withJdbcNormalizer(Class<?> javaType, JdbcNormalizer<?> normalizer) {
        return valueConverter(javaType, (v, s) -> {
            DbAttribute placeholder = new DbAttribute("_placeholder");
            placeholder.setScale(s);
            placeholder.setType(TypesMapping.getSqlTypeByJava(javaType));
            return normalizer.normalize(v, placeholder);
        });
    }

    /**
     * @since 2.4
     */
    public LmRuntimeBuilder extractorResolver(ResourceResolver loader) {
        Objects.requireNonNull(loader);
        this.extractorResolverFactory = () -> loader;
        return this;
    }

    /**
     * @since 1.4
     */
    public LmRuntimeBuilder extractorModelsRoot(File rootDir) {

        Objects.requireNonNull(rootDir);
        if (!rootDir.isDirectory()) {
            LOGGER.warn("Extractor models root is not a valid directory: " + rootDir);
        }

        this.extractorResolverFactory = () -> new FileResourceResolver(rootDir);
        return this;
    }

    /**
     * @since 2.4
     */
    public LmRuntimeBuilder extractorModelsRoot(URL baseUrl) {
        Objects.requireNonNull(baseUrl);
        this.extractorResolverFactory = () -> new URLResourceResolver(baseUrl);
        return this;
    }

    /**
     * @since 1.4
     * @deprecated since 2.4 in favor of {@link #extractorModelsRoot(File)} to avoid confusion between file and URL Strings.
     */
    @Deprecated
    public LmRuntimeBuilder extractorModelsRoot(String rootDirPath) {
        return extractorModelsRoot(new File(rootDirPath));
    }

    public LmRuntime build() throws IllegalStateException {

        if (targetRuntime == null) {
            throw new IllegalStateException("Required Cayenne 'targetRuntime' is not set");
        }

        if (tokenManager == null) {
            tokenManager = new InMemoryTokenManager();
        }

        Module etlModule = binder -> {

            binder.bindMap(Connector.class).putAll(connectors);

            MapBuilder<IConnectorFactory> connectorFactories = binder.bindMap(IConnectorFactory.class)
                    .putAll(LmRuntimeBuilder.this.connectorFactories);

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

            MapBuilder<IExtractorFactory> extractorFactories = binder.bindMap(IExtractorFactory.class);

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

            binder.bindMap(ValueConverter.class).putAll(LmRuntimeBuilder.this.valueConverters);

            // Binding CayenneService for the *target*... Note that binding
            // ServerRuntime directly would result in undesired shutdown
            // when the ETL module is shutdown.
            binder.bind(ITargetCayenneService.class).toInstance(new TargetCayenneService(targetRuntime));

            binder.bind(ResourceResolver.class).toInstance(extractorResolverFactory.get());
            binder.bind(ValueConverterFactory.class).to(ValueConverterFactory.class);
            binder.bind(IExtractorService.class).to(ExtractorService.class);
            binder.bind(IConnectorService.class).to(ConnectorService.class);
            binder.bind(ITaskService.class).to(TaskService.class);
            binder.bind(ITokenManager.class).toInstance(tokenManager);
            binder.bind(IKeyAdapterFactory.class).to(KeyAdapterFactory.class);
            binder.bind(IPathNormalizer.class).to(PathNormalizer.class);
            binder.bind(ITargetPropertyWriterService.class).to(TargetPropertyWriterService.class);
            binder.bind(IExtractorModelService.class).to(ExtractorModelService.class);
            binder.bind(IExtractorModelParser.class).to(ExtractorModelParser.class);

            // apply adapter-contributed bindings
            for (LinkEtlAdapter a : adapters) {
                a.contributeToRuntime(binder);
            }
        };

        Injector injector = DIBootstrap.createInjector(etlModule);

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
}
