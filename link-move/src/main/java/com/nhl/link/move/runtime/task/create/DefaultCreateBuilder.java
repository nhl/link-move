package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.CreateBuilder;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.annotation.*;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.BaseTaskBuilder;
import com.nhl.link.move.runtime.task.ListenersBuilder;
import com.nhl.link.move.runtime.task.common.FkResolver;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.DataObject;

/**
 * @param <T>
 * @since 2.6
 */
public class DefaultCreateBuilder<T extends DataObject> extends BaseTaskBuilder implements CreateBuilder<T> {

    private final CreateTargetMapper<T> mapper;
    private final CreateTargetMerger<T> merger;
    private final FkResolver fkResolver;
    private final ITokenManager tokenManager;
    private final ListenersBuilder stageListenersBuilder;
    private final IExtractorService extractorService;
    private final ITargetCayenneService targetCayenneService;
    private final RowConverter rowConverter;

    private ExtractorName extractorName;

    public DefaultCreateBuilder(
            CreateTargetMapper<T> mapper,
            CreateTargetMerger<T> merger,
            FkResolver fkResolver,
            RowConverter rowConverter,
            ITargetCayenneService targetCayenneService,
            IExtractorService extractorService,
            ITokenManager tokenManager) {

        this.mapper = mapper;
        this.merger = merger;
        this.fkResolver = fkResolver;
        this.tokenManager = tokenManager;
        this.extractorService = extractorService;
        this.targetCayenneService = targetCayenneService;
        this.rowConverter = rowConverter;
        this.stageListenersBuilder = createListenersBuilder();

        // always add stats listener..
        stageListener(CreateStatsListener.instance());
    }

    ListenersBuilder createListenersBuilder() {
        return new ListenersBuilder(
                AfterSourceRowsExtracted.class,
                AfterSourceRowsConverted.class,
                AfterTargetsMapped.class,
                AfterFksResolved.class,
                AfterTargetsMerged.class,
                AfterTargetsCommitted.class);
    }

    @Override
    public CreateBuilder<T> sourceExtractor(String location, String name) {
        this.extractorName = ExtractorName.create(location, name);
        return this;
    }

    @Override
    public CreateBuilder<T> batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public CreateBuilder<T> stageListener(Object listener) {
        stageListenersBuilder.addListener(listener);
        return this;
    }

    @Override
    public LmTask task() throws IllegalStateException {

        if (extractorName == null) {
            throw new IllegalStateException("Required 'extractorName' is not set");
        }

        return new CreateTask<>(extractorName,
                batchSize,
                targetCayenneService,
                extractorService,
                tokenManager,
                createProcessor());
    }

    private CreateSegmentProcessor<T> createProcessor() {
        return new CreateSegmentProcessor<>(
                rowConverter,
                mapper,
                merger,
                fkResolver,
                stageListenersBuilder.getListeners());
    }
}
