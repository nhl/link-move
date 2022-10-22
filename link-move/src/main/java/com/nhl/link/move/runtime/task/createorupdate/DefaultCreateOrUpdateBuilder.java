package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.CreateOrUpdateBuilder;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.annotation.AfterFksResolved;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;
import com.nhl.link.move.annotation.AfterSourcesMapped;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.annotation.AfterTargetsMatched;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.BaseTaskBuilder;
import com.nhl.link.move.runtime.task.MapperBuilder;
import com.nhl.link.move.runtime.task.common.FkResolver;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.Persistent;
import org.apache.cayenne.exp.property.Property;

import java.lang.annotation.Annotation;

/**
 * A builder of an ETL task that matches source data with target data based on a certain unique attribute on both sides.
 */
public class DefaultCreateOrUpdateBuilder<T extends Persistent> extends BaseTaskBuilder<DefaultCreateOrUpdateBuilder<T>> implements CreateOrUpdateBuilder<T> {

    private final Class<T> type;
    private final CreateOrUpdateTargetMerger merger;
    private final IExtractorService extractorService;
    private final ITargetCayenneService targetCayenneService;
    private final ITokenManager tokenManager;
    private final RowConverter rowConverter;
    private final MapperBuilder mapperBuilder;
    private final FkResolver fkResolver;

    private Mapper mapper;
    private ExtractorName extractorName;

    public DefaultCreateOrUpdateBuilder(
            Class<T> type,
            CreateOrUpdateTargetMerger merger,
            FkResolver fkResolver,
            RowConverter rowConverter,
            ITargetCayenneService targetCayenneService,
            IExtractorService extractorService,
            ITokenManager tokenManager,
            MapperBuilder mapperBuilder,
            LmLogger logger) {

        super(logger);

        this.type = type;
        this.merger = merger;
        this.fkResolver = fkResolver;
        this.targetCayenneService = targetCayenneService;
        this.extractorService = extractorService;
        this.tokenManager = tokenManager;
        this.rowConverter = rowConverter;
        this.mapperBuilder = mapperBuilder;

        // always add stats listener
        stageListener(CreateOrUpdateStatsListener.instance());
    }

    @Override
    protected Class<? extends Annotation>[] supportedListenerAnnotations() {
        return new Class[]{AfterSourceRowsExtracted.class,
                AfterSourceRowsConverted.class,
                AfterSourcesMapped.class,
                AfterTargetsMatched.class,
                AfterTargetsMapped.class,
                AfterFksResolved.class,
                AfterTargetsMerged.class,
                AfterTargetsCommitted.class};
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
                createProcessor(),
                logger);
    }

    private CreateOrUpdateSegmentProcessor createProcessor() {

        Mapper mapper = this.mapper != null ? this.mapper : mapperBuilder.build();

        return new CreateOrUpdateSegmentProcessor(
                rowConverter,
                new SourceMapper(mapper),
                new CreateOrUpdateTargetMatcher(type, mapper),
                new CreateOrUpdateTargetMapper(type, mapper),
                merger,
                fkResolver,
                getListeners());
    }
}
