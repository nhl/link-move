package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.CreateBuilder;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.BaseTaskBuilder;
import com.nhl.link.move.runtime.task.ListenersBuilder;
import com.nhl.link.move.runtime.task.createorupdate.LegacyRowConverter;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.DataObject;

/**
 * @param <T>
 * @since 2.6
 */
public class DefaultCreateBuilder<T extends DataObject> extends BaseTaskBuilder implements CreateBuilder<T> {

    private CreateTargetMapper<T> mapper;
    private CreateTargetMerger<T> merger;
    private ITokenManager tokenManager;
    private ExtractorName extractorName;
    private ListenersBuilder stageListenersBuilder;
    private IExtractorService extractorService;
    private ITargetCayenneService targetCayenneService;
    private LegacyRowConverter rowConverter;

    public DefaultCreateBuilder(
            CreateTargetMapper<T> mapper,
            CreateTargetMerger<T> merger,
            LegacyRowConverter rowConverter,
            ITargetCayenneService targetCayenneService,
            IExtractorService extractorService,
            ITokenManager tokenManager) {

        this.mapper = mapper;
        this.merger = merger;
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
                AfterSourceRowsConverted.class,
                AfterTargetsMapped.class,
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
        return new CreateSegmentProcessor<>(rowConverter, mapper, merger, stageListenersBuilder.getListeners());
    }
}
