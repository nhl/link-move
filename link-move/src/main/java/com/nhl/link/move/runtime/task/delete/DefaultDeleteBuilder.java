package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.DeleteBuilder;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.annotation.AfterMissingTargetsFiltered;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsExtracted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.task.BaseTaskBuilder;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.MapperBuilder;
import com.nhl.link.move.runtime.task.common.StatsIncrementor;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.property.Property;

import java.lang.annotation.Annotation;

/**
 * @since 1.3
 */
public class DefaultDeleteBuilder extends BaseTaskBuilder<DefaultDeleteBuilder, DeleteSegment, DeleteStage> implements DeleteBuilder {

    private final ITaskService taskService;
    private final ITargetCayenneService targetCayenneService;
    private final Class<?> type;
    private final MapperBuilder mapperBuilder;

    private Expression targetFilter;
    private ExtractorName extractorName;
    private Mapper mapper;

    public DefaultDeleteBuilder(
            Class<?> type,
            ITargetCayenneService targetCayenneService,
            ITaskService taskService,
            MapperBuilder mapperBuilder,
            LmLogger logger) {

        super(logger);

        this.taskService = taskService;
        this.targetCayenneService = targetCayenneService;
        this.type = type;
        this.mapperBuilder = mapperBuilder;

        setupStatsCallbacks();
    }

    protected void setupStatsCallbacks() {
        StatsIncrementor incrementor = StatsIncrementor.instance();
        stage(DeleteStage.EXTRACT_TARGET, incrementor::targetsExtracted);
        stage(DeleteStage.COMMIT_TARGET, incrementor::targetsCommitted);
    }

    @Override
    protected Class<? extends Annotation>[] supportedListenerAnnotations() {
        return new Class[]{
                AfterTargetsExtracted.class,
                AfterTargetsMapped.class,
                AfterMissingTargetsFiltered.class,
                AfterTargetsCommitted.class
        };
    }

    @Override
    public DefaultDeleteBuilder targetFilter(Expression targetFilter) {
        this.targetFilter = targetFilter;
        return this;
    }

    @Override
    public DeleteBuilder sourceMatchExtractor(String location, String name) {
        this.extractorName = ExtractorName.create(location, name);
        return this;
    }

    @Override
    public DefaultDeleteBuilder matchBy(Mapper mapper) {
        this.mapper = mapper;
        return this;
    }

    @Override
    public DefaultDeleteBuilder matchBy(String... keyAttributes) {
        this.mapper = null;
        this.mapperBuilder.matchBy(keyAttributes);
        return this;
    }

    @Override
    public DefaultDeleteBuilder matchBy(Property<?>... matchAttributes) {
        this.mapper = null;
        this.mapperBuilder.matchBy(matchAttributes);
        return this;
    }

    @Override
    public DefaultDeleteBuilder matchById() {
        this.mapper = null;
        this.mapperBuilder.matchById();
        return this;
    }

    @Override
    public DeleteTask task() throws IllegalStateException {

        Mapper mapper = this.mapper != null ? this.mapper : mapperBuilder.build();

        return new DeleteTask(
                batchSize,
                type,
                targetFilter,
                targetCayenneService,
                createSourceKeysSubtask(mapper),
                createProcessor(mapper),
                logger);
    }

    private LmTask createSourceKeysSubtask(Mapper mapper) {
        return taskService.extractSourceKeys(type).sourceExtractor(extractorName).matchBy(mapper).task();
    }

    private DeleteSegmentProcessor createProcessor(Mapper mapper) {
        return new DeleteSegmentProcessor(
                new TargetMapper(mapper),
                new MissingTargetsFilterStage(),
                new DeleteTargetStage(),
                getCallbackExecutor());
    }
}
