package com.nhl.link.move.runtime;

import com.nhl.link.move.connect.Connector;
import com.nhl.link.move.extractor.parser.ExtractorModelParser;
import com.nhl.link.move.extractor.parser.IExtractorModelParser;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.log.Slf4jLmLogger;
import com.nhl.link.move.resource.ClasspathResourceResolver;
import com.nhl.link.move.resource.FolderResourceResolver;
import com.nhl.link.move.resource.ResourceResolver;
import com.nhl.link.move.resource.URLResourceResolver;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.cayenne.TargetCayenneService;
import com.nhl.link.move.runtime.cayenne.TargetConnectorFactory;
import com.nhl.link.move.runtime.connect.ConnectorInstanceFactory;
import com.nhl.link.move.runtime.connect.ConnectorServiceProvider;
import com.nhl.link.move.runtime.connect.IConnectorFactory;
import com.nhl.link.move.runtime.connect.IConnectorService;
import com.nhl.link.move.runtime.extractor.ExtractorService;
import com.nhl.link.move.runtime.extractor.IExtractorFactory;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.extractor.model.ExtractorModelService;
import com.nhl.link.move.runtime.extractor.model.IExtractorModelService;
import com.nhl.link.move.runtime.jdbc.JdbcExtractorFactory;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.key.KeyAdapterFactory;
import com.nhl.link.move.runtime.targetmodel.DefaultTargetEntityMap;
import com.nhl.link.move.runtime.targetmodel.TargetEntityMap;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.TaskService;
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
import org.apache.cayenne.di.Binder;
import org.apache.cayenne.di.DIBootstrap;
import org.apache.cayenne.di.Injector;
import org.apache.cayenne.di.ListBuilder;
import org.apache.cayenne.di.MapBuilder;
import org.apache.cayenne.di.Module;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A builder class that helps to assemble working LinkEtl stack.
 */
public class LmRuntimeBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(LmRuntimeBuilder.class);

    private final Set<IConnectorFactory> connectorFactories;
    private final Set<Class<? extends IConnectorFactory<? extends Connector>>> connectorFactoryTypes;

    private final Map<String, IExtractorFactory> extractorFactories;
    private final Map<String, Class<? extends IExtractorFactory<?>>> extractorFactoryTypes;

    private final Map<String, ValueConverter> valueConverters;
    private Supplier<ResourceResolver> extractorResolverFactory;

    private ServerRuntime targetRuntime;
    private final Collection<LmAdapter> adapters;

    protected LmRuntimeBuilder() {
        this.connectorFactories = new HashSet<>();
        this.connectorFactoryTypes = new HashSet<>();
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
    public LmRuntimeBuilder adapter(LmAdapter adapter) {
        this.adapters.add(adapter);
        return this;
    }

    /**
     * Sets a target Cayenne runtime for this ETL stack.
     *
     * @since 3.0.0
     */
    public LmRuntimeBuilder targetRuntime(ServerRuntime targetRuntime) {
        this.targetRuntime = targetRuntime;
        return this;
    }

    /**
     * @since 3.0.0
     */
    public <C extends Connector> LmRuntimeBuilder connector(
            Class<C> connectorType,
            String id,
            C connector) {
        return connectorFactory(new ConnectorInstanceFactory(connectorType, id, connector));
    }

    /**
     * @since 3.0.0
     */
    public <C extends Connector> LmRuntimeBuilder connectorFactory(IConnectorFactory<C> factory) {
        connectorFactories.add(factory);
        return this;
    }

    /**
     * @since 3.0.0
     */
    public <C extends Connector> LmRuntimeBuilder connectorFactory(Class<? extends IConnectorFactory<C>> factoryType) {
        connectorFactoryTypes.add(factoryType);
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
    public LmRuntimeBuilder connectorFromTarget() {
        return connectorFactory(TargetConnectorFactory.class);
    }

    /**
     * Adds an extra factory to the map of extractor factories. Note that {@link JdbcExtractorFactory} is loaded by
     * default and does not have to be configured explicitly.
     *
     * @since 3.0.0
     */
    public LmRuntimeBuilder extractorFactory(String extractorType, Class<? extends IExtractorFactory<?>> factoryType) {
        extractorFactoryTypes.put(extractorType, factoryType);
        return this;
    }

    /**
     * Adds an extra factory to the map of extractor factories. Note that
     * {@link JdbcExtractorFactory} is loaded by default and does not have to be
     * configured explicitly.
     */
    public LmRuntimeBuilder extractorFactory(String extractorType, IExtractorFactory<?> factory) {
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

        Injector injector = DIBootstrap.createInjector(new LmRuntimeModule());
        return new DefaultLmRuntime(injector);
    }

    class LmRuntimeModule implements Module {

        @Override
        public void configure(Binder binder) {

            bindConnectorFactories(binder);
            bindExtractorFactories(binder);
            bindValueConverters(binder);
            bindCayenneService(binder);

            binder.bind(LmLogger.class).to(Slf4jLmLogger.class);
            binder.bind(ResourceResolver.class).toInstance(extractorResolverFactory.get());
            binder.bind(ValueConverterFactory.class).to(ValueConverterFactory.class);
            binder.bind(IExtractorService.class).to(ExtractorService.class);
            binder.bind(IConnectorService.class).toProvider(ConnectorServiceProvider.class);
            binder.bind(ITaskService.class).to(TaskService.class);
            binder.bind(IKeyAdapterFactory.class).to(KeyAdapterFactory.class);
            binder.bind(TargetEntityMap.class).to(DefaultTargetEntityMap.class);
            binder.bind(ITargetPropertyWriterService.class).to(TargetPropertyWriterService.class);
            binder.bind(IExtractorModelService.class).to(ExtractorModelService.class);
            binder.bind(IExtractorModelParser.class).to(ExtractorModelParser.class);

            // bind adapter-provided services AFTER all the defaults are bound, so they can override services if needed
            bindAdapters(binder);
        }

        private void bindConnectorFactories(Binder binder) {
            ListBuilder<IConnectorFactory> cfs = binder.bindList(IConnectorFactory.class);

            cfs.addAll(connectorFactories);

            connectorFactoryTypes.forEach(c -> {
                // A bit ugly - need to bind all factory types explicitly before placing them in a map.
                // Also, must drop parameterization to be able to bind with non-specific boundaries (<? extends ...>)
                Class efType = c;
                binder.bind(efType).to(efType);

                cfs.add(c);
            });
        }

        private void bindExtractorFactories(Binder binder) {
            MapBuilder<IExtractorFactory> efMap = binder.bindMap(IExtractorFactory.class);

            ServiceLoader.load(IExtractorFactory.class).forEach(ef -> efMap.put(ef.getExtractorType(), ef));
            efMap.putAll(extractorFactories);
            extractorFactoryTypes.forEach((n, c) -> {

                // A bit ugly - need to bind all factory types explicitly before placing then in a map.
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
            adapters.forEach(a -> a.configure(binder));
        }
    }


}
