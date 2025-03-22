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
import com.nhl.link.move.runtime.task.common.StatsIncrementor;
import org.apache.cayenne.exp.property.Property;

import java.lang.annotation.Annotation;

/**
 * A builder of an ETL task that matches source data with target data based on a certain unique attribute on both sides.
 */
public class DefaultCreateOrUpdateBuilder extends BaseTaskBuilder<DefaultCreateOrUpdateBuilder, CreateOrUpdateSegment, CreateOrUpdateStage> implements CreateOrUpdateBuilder {

    private final Class<?> type;
    private final CreateOrUpdateTargetMerger merger;
    private final IExtractorService extractorService;
    private final ITargetCayenneService targetCayenneService;
    private final RowConverter rowConverter;
    private final MapperBuilder mapperBuilder;
    private final FkResolver fkResolver;

    private Mapper mapper;
    private ExtractorName extractorName;

    public DefaultCreateOrUpdateBuilder(
            Class<?> type,
            CreateOrUpdateTargetMerger merger,
            FkResolver fkResolver,
            RowConverter rowConverter,
            ITargetCayenneService targetCayenneService,
            IExtractorService extractorService,
            MapperBuilder mapperBuilder,
            LmLogger logger) {

        super(logger);

        this.type = type;
        this.merger = merger;
        this.fkResolver = fkResolver;
        this.targetCayenneService = targetCayenneService;
        this.extractorService = extractorService;
        this.rowConverter = rowConverter;
        this.mapperBuilder = mapperBuilder;

        setupStatsCallbacks();
    }

    protected void setupStatsCallbacks() {
        StatsIncrementor incrementor = StatsIncrementor.instance();
        stage(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, incrementor::sourceRowsExtracted);
        stage(CreateOrUpdateStage.COMMIT_TARGET, incrementor::targetsCommitted);
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
    public DefaultCreateOrUpdateBuilder sourceExtractor(String location, String name) {
        this.extractorName = ExtractorName.create(location, name);
        return this;
    }

    @Override
    public DefaultCreateOrUpdateBuilder matchBy(Mapper mapper) {
        this.mapper = mapper;
        return this;
    }

    @Override
    public DefaultCreateOrUpdateBuilder matchBy(String... keyAttributes) {
        this.mapper = null;
        this.mapperBuilder.matchBy(keyAttributes);
        return this;
    }

    /**
     * @since 1.1
     */
    @Override
    public DefaultCreateOrUpdateBuilder matchBy(Property<?>... matchAttributes) {
        this.mapper = null;
        this.mapperBuilder.matchBy(matchAttributes);
        return this;
    }

    /**
     * @since 1.4
     */
    @Override
    public DefaultCreateOrUpdateBuilder matchById() {
        this.mapper = null;
        this.mapperBuilder.matchById();
        return this;
    }

    @Override
    public LmTask task() throws IllegalStateException {

        if (extractorName == null) {
            throw new IllegalStateException("Required 'extractorName' is not set");
        }

        return new CreateOrUpdateTask(
                extractorName,
                batchSize,
                targetCayenneService,
                extractorService,
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
                getCallbackExecutor());
    }
}
