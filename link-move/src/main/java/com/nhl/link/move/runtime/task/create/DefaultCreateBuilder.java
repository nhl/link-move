package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.CreateBuilder;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.BaseTaskBuilder;
import com.nhl.link.move.runtime.task.common.FkResolver;
import com.nhl.link.move.runtime.task.common.StatsIncrementor;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;

/**
 * @since 2.6
 */
public class DefaultCreateBuilder extends BaseTaskBuilder<DefaultCreateBuilder, CreateSegment, CreateStage> implements CreateBuilder {

    private final CreateTargetMapper mapper;
    private final CreateTargetMerger merger;
    private final FkResolver fkResolver;
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
            LmLogger logger) {

        super(logger);
        this.mapper = mapper;
        this.merger = merger;
        this.fkResolver = fkResolver;
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
