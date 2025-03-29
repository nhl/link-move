package com.nhl.link.move.runtime.task;

import com.nhl.link.move.Execution;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.task.common.CallbackExecutor;
import com.nhl.link.move.runtime.task.common.DataSegment;
import com.nhl.link.move.runtime.task.common.TaskStageType;

import java.util.function.BiConsumer;

/**
 * A common superclass of various task builder.
 *
 * @since 1.3
 */
public abstract class BaseTaskBuilder<T extends BaseTaskBuilder<T, S, U>, S extends DataSegment, U extends TaskStageType> {

    private static final int DEFAULT_BATCH_SIZE = 500;

    protected final LmLogger logger;
    protected final ListenersBuilder<S, U> stageListenersBuilder;

    protected int batchSize;

    public BaseTaskBuilder(LmLogger logger) {
        this.logger = logger;
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.stageListenersBuilder = new ListenersBuilder<>();
    }

    /**
     * @since 3.0.0
     */
    public T stage(U stage, BiConsumer<Execution, S> callback) {
        stageListenersBuilder.addStageCallback(stage, callback);
        return (T) this;
    }

    public T batchSize(int batchSize) {
        this.batchSize = batchSize;
        return (T) this;
    }

    protected CallbackExecutor<U, S> getCallbackExecutor() {
        return stageListenersBuilder.getCallbackExecutor();
    }
}
