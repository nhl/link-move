package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.CreateOrUpdateBuilder;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterSourcesMapped;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.annotation.AfterTargetsMatched;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.BaseTaskBuilder;
import com.nhl.link.move.runtime.task.ListenersBuilder;
import com.nhl.link.move.runtime.task.MapperBuilder;
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

    private Class<T> type;
    private CreateOrUpdateTargetMerger<T> merger;
    private IExtractorService extractorService;
    private ITargetCayenneService targetCayenneService;
    private ITokenManager tokenManager;
    private RowConverter rowConverter;
    private MapperBuilder mapperBuilder;
    private Mapper mapper;
    private ListenersBuilder stageListenersBuilder;
    private ExtractorName extractorName;

    public DefaultCreateOrUpdateBuilder(
            Class<T> type,
            CreateOrUpdateTargetMerger<T> merger,
            RowConverter rowConverter,
            ITargetCayenneService targetCayenneService,
            IExtractorService extractorService,
            ITokenManager tokenManager,
            MapperBuilder mapperBuilder) {

        this.type = type;
        this.merger = merger;
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
                AfterSourceRowsConverted.class,
                AfterSourcesMapped.class,
                AfterTargetsMatched.class,
                AfterTargetsMapped.class,
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

        return new CreateOrUpdateTask<T>(extractorName, batchSize, targetCayenneService, extractorService, tokenManager,
                createProcessor());
    }

    private CreateOrUpdateSegmentProcessor<T> createProcessor() {

        Mapper mapper = this.mapper != null ? this.mapper : mapperBuilder.build();

        SourceMapper sourceMapper = new SourceMapper(mapper);
        TargetMatcher<T> targetMatcher = new TargetMatcher<>(type, mapper);
        TargetMapper<T> targetMapper = new TargetMapper<>(type, mapper);

        return new CreateOrUpdateSegmentProcessor<>(
                rowConverter,
                sourceMapper,
                targetMatcher,
                targetMapper,
                merger,
                stageListenersBuilder.getListeners());
    }
}
