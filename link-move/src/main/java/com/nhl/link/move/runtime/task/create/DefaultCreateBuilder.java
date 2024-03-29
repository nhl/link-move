package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.CreateBuilder;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.annotation.AfterFksResolved;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.BaseTaskBuilder;
import com.nhl.link.move.runtime.task.common.FkResolver;
import com.nhl.link.move.runtime.task.common.StatsIncrementor;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;
import com.nhl.link.move.runtime.token.ITokenManager;

import java.lang.annotation.Annotation;

/**
 * @since 2.6
 */
public class DefaultCreateBuilder extends BaseTaskBuilder<DefaultCreateBuilder, CreateSegment, CreateStage> implements CreateBuilder {

    private final CreateTargetMapper mapper;
    private final CreateTargetMerger merger;
    private final FkResolver fkResolver;
    private final ITokenManager tokenManager;
    private final IExtractorService extractorService;
    private final ITargetCayenneService targetCayenneService;
    private final RowConverter rowConverter;

    private ExtractorName extractorName;

    public DefaultCreateBuilder(
            CreateTargetMapper mapper,
            CreateTargetMerger merger,
            FkResolver fkResolver,
            RowConverter rowConverter,
            ITargetCayenneService targetCayenneService,
            IExtractorService extractorService,
            ITokenManager tokenManager,
            LmLogger logger) {

        super(logger);
        this.mapper = mapper;
        this.merger = merger;
        this.fkResolver = fkResolver;
        this.tokenManager = tokenManager;
        this.extractorService = extractorService;
        this.targetCayenneService = targetCayenneService;
        this.rowConverter = rowConverter;

        setupStatsCallbacks();
    }

    protected void setupStatsCallbacks() {
        StatsIncrementor incrementor = StatsIncrementor.instance();
        stage(CreateStage.EXTRACT_SOURCE_ROWS, incrementor::sourceRowsExtracted);
        stage(CreateStage.COMMIT_TARGET, incrementor::targetsCommitted);
    }

    @Override
    protected Class<? extends Annotation>[] supportedListenerAnnotations() {
        return new Class[]{
                AfterSourceRowsExtracted.class,
                AfterSourceRowsConverted.class,
                AfterTargetsMapped.class,
                AfterFksResolved.class,
                AfterTargetsMerged.class,
                AfterTargetsCommitted.class};
    }

    @Override
    public CreateBuilder sourceExtractor(String location, String name) {
        this.extractorName = ExtractorName.create(location, name);
        return this;
    }

    @Override
    public LmTask task() throws IllegalStateException {

        if (extractorName == null) {
            throw new IllegalStateException("Required 'extractorName' is not set");
        }

        return new CreateTask(extractorName,
                batchSize,
                targetCayenneService,
                extractorService,
                tokenManager,
                createProcessor(),
                logger);
    }

    private CreateSegmentProcessor createProcessor() {
        return new CreateSegmentProcessor(
                rowConverter,
                mapper,
                merger,
                fkResolver,
                getCallbackExecutor());
    }
}
