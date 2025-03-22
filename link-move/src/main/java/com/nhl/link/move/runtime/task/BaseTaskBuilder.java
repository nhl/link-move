package com.nhl.link.move.runtime.task;

import com.nhl.link.move.Execution;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.task.common.CallbackExecutor;
import com.nhl.link.move.runtime.task.common.DataSegment;
import com.nhl.link.move.runtime.task.common.TaskStageType;

import java.lang.annotation.Annotation;
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
        this.stageListenersBuilder = new ListenersBuilder<>(supportedListenerAnnotations());
    }

    @Deprecated(since = "3.0.0", forRemoval = true)
    protected abstract Class<? extends Annotation>[] supportedListenerAnnotations();

    /**
     * @deprecated use lambda-based callbacks instead, @see {@link com.nhl.link.move.runtime.task.BaseTaskBuilder#stage}
     */
    @Deprecated(since = "3.0.0", forRemoval = true)
    public T stageListener(Object listener) {
        stageListenersBuilder.addListener(listener);
        return (T) this;
    }

    /**
     * @since 3.0.0
     */
    public T stage(U stageType, BiConsumer<Execution, S> callback) {
        stageListenersBuilder.addStageCallback(stageType, callback);
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
