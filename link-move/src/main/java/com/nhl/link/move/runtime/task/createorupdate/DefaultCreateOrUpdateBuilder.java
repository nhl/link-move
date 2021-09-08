package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.CreateOrUpdateBuilder;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.annotation.*;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.BaseTaskBuilder;
import com.nhl.link.move.runtime.task.ListenersBuilder;
import com.nhl.link.move.runtime.task.MapperBuilder;
import com.nhl.link.move.runtime.task.common.FkResolver;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.Property;

/**
 * A builder of an ETL task that matches source data with target data based on a
 * certain unique attribute on both sides.
 */
public class DefaultCreateOrUpdateBuilder<T extends DataObject>
        extends BaseTaskBuilder
        implements CreateOrUpdateBuilder<T> {

    private final Class<T> type;
    private final CreateOrUpdateTargetMerger<T> merger;
    private final IExtractorService extractorService;
    private final ITargetCayenneService targetCayenneService;
    private final ITokenManager tokenManager;
    private final RowConverter rowConverter;
    private final MapperBuilder mapperBuilder;
    private Mapper mapper;
    private final FkResolver fkResolver;
    private final ListenersBuilder stageListenersBuilder;
    private ExtractorName extractorName;

    public DefaultCreateOrUpdateBuilder(
            Class<T> type,
            CreateOrUpdateTargetMerger<T> merger,
            FkResolver fkResolver,
            RowConverter rowConverter,
            ITargetCayenneService targetCayenneService,
            IExtractorService extractorService,
            ITokenManager tokenManager,
            MapperBuilder mapperBuilder) {

        this.type = type;
        this.merger = merger;
        this.fkResolver = fkResolver;
        this.targetCayenneService = targetCayenneService;
        this.extractorService = extractorService;
        this.tokenManager = tokenManager;
        this.rowConverter = rowConverter;
        this.mapperBuilder = mapperBuilder;
        this.stageListenersBuilder = createListenersBuilder();

        // always add stats listener..
        stageListener(CreateOrUpdateStatsListener.instance());
    }

    ListenersBuilder createListenersBuilder() {
        return new ListenersBuilder(
                AfterSourceRowsExtracted.class,
                AfterSourceRowsConverted.class,
                AfterSourcesMapped.class,
                AfterTargetsMatched.class,
                AfterTargetsMapped.class,
                AfterFksResolved.class,
                AfterTargetsMerged.class,
                AfterTargetsCommitted.class);
    }

    @Override
    public DefaultCreateOrUpdateBuilder<T> sourceExtractor(String location, String name) {
        this.extractorName = ExtractorName.create(location, name);
        return this;
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

        return new CreateOrUpdateTask<>(
                extractorName,
                batchSize,
                targetCayenneService,
                extractorService,
                tokenManager,
                createProcessor());
    }

    private CreateOrUpdateSegmentProcessor<T> createProcessor() {

        Mapper mapper = this.mapper != null ? this.mapper : mapperBuilder.build();

        SourceMapper sourceMapper = new SourceMapper(mapper);
        CreateOrUpdateTargetMatcher<T> targetMatcher = new CreateOrUpdateTargetMatcher<>(type, mapper);
        CreateOrUpdateTargetMapper<T> targetMapper = new CreateOrUpdateTargetMapper<>(type, mapper);

        return new CreateOrUpdateSegmentProcessor<>(
                rowConverter,
                sourceMapper,
                targetMatcher,
                targetMapper,
                merger,
                fkResolver,
                stageListenersBuilder.getListeners());
    }
}
