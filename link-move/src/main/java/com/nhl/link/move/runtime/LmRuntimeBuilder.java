package com.nhl.link.move.runtime;

import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.extractor.parser.ExtractorModelParser;
import com.nhl.link.move.extractor.parser.IExtractorModelParser;
import com.nhl.link.move.resource.ClasspathResourceResolver;
import com.nhl.link.move.resource.FolderResourceResolver;
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
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.key.KeyAdapterFactory;
import com.nhl.link.move.runtime.targetmodel.DefaultTargetEntityMap;
import com.nhl.link.move.runtime.targetmodel.TargetEntityMap;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.TaskService;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.runtime.token.InMemoryTokenManager;
import com.nhl.link.move.valueconverter.*;
import com.nhl.link.move.writer.ITargetPropertyWriterService;
import com.nhl.link.move.writer.TargetPropertyWriterService;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.di.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.function.Supplier;

/**
 * A builder class that helps to assemble working LinkEtl stack.
 */
public class LmRuntimeBuilder {

    public static final String START_TOKEN_VAR = "startToken";
    public static final String END_TOKEN_VAR = "endToken";
    private static final Logger LOGGER = LoggerFactory.getLogger(LmRuntimeBuilder.class);
    private final Map<String, Connector> connectors;
    private final Map<String, IConnectorFactory> connectorFactories;
    private final Map<String, Class<? extends IConnectorFactory<? extends Connector>>> connectorFactoryTypes;

    private final Map<String, IExtractorFactory> extractorFactories;
    private final Map<String, Class<? extends IExtractorFactory<?>>> extractorFactoryTypes;

    private final Map<String, ValueConverter> valueConverters;
    private Supplier<ResourceResolver> extractorResolverFactory;

    private ITokenManager tokenManager;
    private ServerRuntime targetRuntime;
    private final Collection<LinkEtlAdapter> adapters;

    public LmRuntimeBuilder() {
        this.connectors = new HashMap<>();
        this.connectorFactories = new HashMap<>();
        this.connectorFactoryTypes = new HashMap<>();
        this.extractorFactories = new HashMap<>();
        this.extractorFactoryTypes = new HashMap<>();
        this.valueConverters = createDefaultValueConverters();
        this.adapters = new ArrayList<>();
        this.extractorResolverFactory = ClasspathResourceResolver::new;
    }

    protected Map<String, ValueConverter> createDefaultValueConverters() {
        Map<String, ValueConverter> converters = new HashMap<>();

        converters.put(Boolean.class.getName(), BooleanConverter.getConverter());
        converters.put(Boolean.TYPE.getName(), BooleanConverter.getConverter());
        converters.put(Long.class.getName(), LongConverter.getConverter());
        converters.put(Long.TYPE.getName(), LongConverter.getConverter());
        converters.put(Integer.class.getName(), IntegerConverter.getConverter());
        converters.put(Integer.TYPE.getName(), IntegerConverter.getConverter());
        converters.put(BigDecimal.class.getName(), new BigDecimalConverter());
        converters.put(LocalDate.class.getName(), new LocalDateConverter());
        converters.put(LocalTime.class.getName(), new LocalTimeConverter());
        converters.put(LocalDateTime.class.getName(), new LocalDateTimeConverter());
        converters.put(String.class.getName(), new StringConverter());

        return converters;
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

        this.extractorResolverFactory = () -> new FolderResourceResolver(rootDir);
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

    public LmRuntime build() throws IllegalStateException {

        if (targetRuntime == null) {
            throw new IllegalStateException("Required Cayenne 'targetRuntime' is not set");
        }

        if (tokenManager == null) {
            tokenManager = new InMemoryTokenManager();
        }

        Injector injector = DIBootstrap.createInjector(new LmRuntimeModule());
        return new DefaultLmRuntime(injector);
    }

    class LmRuntimeModule implements Module {

        @Override
        public void configure(Binder binder) {

            bindConnectors(binder);
            bindConnectorFactories(binder);
            bindExtractorFactories(binder);
            bindValueConverters(binder);
            bindCayenneService(binder);

            binder.bind(ResourceResolver.class).toInstance(extractorResolverFactory.get());
            binder.bind(ValueConverterFactory.class).to(ValueConverterFactory.class);
            binder.bind(IExtractorService.class).to(ExtractorService.class);
            binder.bind(IConnectorService.class).to(ConnectorService.class);
            binder.bind(ITaskService.class).to(TaskService.class);
            binder.bind(ITokenManager.class).toInstance(tokenManager);
            binder.bind(IKeyAdapterFactory.class).to(KeyAdapterFactory.class);
            binder.bind(TargetEntityMap.class).to(DefaultTargetEntityMap.class);
            binder.bind(ITargetPropertyWriterService.class).to(TargetPropertyWriterService.class);
            binder.bind(IExtractorModelService.class).to(ExtractorModelService.class);
            binder.bind(IExtractorModelParser.class).to(ExtractorModelParser.class);

            // bind adapter-provided services AFTER all the defaults are bound, so they can override services if needed
            bindAdapters(binder);
        }

        private void bindConnectors(Binder binder) {
            binder.bindMap(Connector.class).putAll(connectors);
        }

        private void bindConnectorFactories(Binder binder) {
            MapBuilder<IConnectorFactory> cfMap = binder.bindMap(IConnectorFactory.class);

            cfMap.putAll(connectorFactories);

            connectorFactoryTypes.forEach((n, c) -> {
                // a bit ugly - need to bind all factory types explicitly before placing then in a map ..
                // also must drop parameterization to be able to bind with non-specific boundaries (<? extends ...>)
                Class efType = c;
                binder.bind(efType).to(efType);

                cfMap.put(n, c);
            });
        }

        private void bindExtractorFactories(Binder binder) {
            MapBuilder<IExtractorFactory> efMap = binder.bindMap(IExtractorFactory.class);

            ServiceLoader.load(IExtractorFactory.class).forEach(ef -> efMap.put(ef.getExtractorType(), ef));
            efMap.putAll(extractorFactories);
            extractorFactoryTypes.forEach((n, c) -> {

                // a bit ugly - need to bind all factory types explicitly before placing then in a map ..
                // also must drop parameterization to be able to bind with non-specific boundaries (<? extends ...>)
                Class efType = c;
                binder.bind(efType).to(efType);
                efMap.put(n, c);
            });
        }

        private void bindValueConverters(Binder binder) {
            binder.bindMap(ValueConverter.class).putAll(valueConverters);
        }

        private void bindCayenneService(Binder binder) {
            // Binding CayenneService for the *target*...
            // Note that binding ServerRuntime directly would result in undesired shutdown when the ETL module is shutdown.
            binder.bind(ITargetCayenneService.class).toInstance(new TargetCayenneService(targetRuntime));
        }

        private void bindAdapters(Binder binder) {
            adapters.forEach(a -> a.contributeToRuntime(binder));
        }
    }


}
