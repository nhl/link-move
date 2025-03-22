package com.nhl.link.move.runtime.task.common;

import com.nhl.link.move.Execution;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Stores and executes lambda-based callbacks together with deprecated annotated job callbacks (listeners)
 *
 * @since 3.0.0
 */
public class CallbackExecutor<T extends TaskStageType, D extends DataSegment> {

    private final Map<T, List<BiConsumer<Execution, D>>> callbacks;

    public CallbackExecutor(Map<T, List<BiConsumer<Execution, D>>> callbacks) {
        this.callbacks = callbacks;
    }

    public void executeCallbacks(T stageType, Execution exec, D segment) {
        List<BiConsumer<Execution, D>> stageCallbacks = callbacks.get(stageType);
        if (stageCallbacks != null) {
            for (BiConsumer<Execution, D> callback : stageCallbacks) {
                callback.accept(exec, segment);
            }
        }
    }
}
