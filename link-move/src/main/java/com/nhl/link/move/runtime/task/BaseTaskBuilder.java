package com.nhl.link.move.runtime.task;

import com.nhl.link.move.log.LmLogger;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

/**
 * A common superclass of various task builder.
 *
 * @since 1.3
 */
public abstract class BaseTaskBuilder<T extends BaseTaskBuilder<T>> {

    private static final int DEFAULT_BATCH_SIZE = 500;

    protected final LmLogger logger;
    protected final ListenersBuilder stageListenersBuilder;

    protected int batchSize;

    public BaseTaskBuilder(LmLogger logger) {
        this.logger = logger;
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.stageListenersBuilder = new ListenersBuilder(supportedListenerAnnotations());
    }

    protected abstract Class<? extends Annotation>[] supportedListenerAnnotations();

    public T stageListener(Object listener) {
        stageListenersBuilder.addListener(listener);
        return (T) this;
    }

    public T batchSize(int batchSize) {
        this.batchSize = batchSize;
        return (T) this;
    }

    protected Map<Class<? extends Annotation>, List<StageListener>> getListeners() {
        return stageListenersBuilder.getListeners();
    }
}
