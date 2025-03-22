package com.nhl.link.move;

import java.util.Collections;
import java.util.Map;

/**
 * A runnable data transfer task.
 */
public interface LmTask {

    /**
     * Executes the task returning {@link Execution} object that can be used by
     * the caller to analyze the results. Currently, all task implementations are
     * synchronous, so this method returns only on task completion.
     *
     * @since 1.1
     */
    default Execution run() {
        return run(Collections.emptyMap());
    }

    /**
     * Executes the task with a map of parameters returning {@link Execution}
     * object that can be used by the caller to analyze the results. Currently,
     * all task implementations are synchronous, so this method returns only on
     * task completion.
     *
     * @since 1.3
     */
    Execution run(Map<String, ?> params);

    /**
     * A flavor of "run" that allows tasks to start subtasks within their own execution.
     *
     * @since 3.0.0
     */
    Execution run(Map<String, ?> params, Execution parentExec);

    /**
     * @deprecated as we are no longer planning to support {@link SyncToken}
     */
    @Deprecated(since = "3.0.0", forRemoval = true)
    default Execution run(SyncToken token) {
        return run(token, Collections.emptyMap());
    }

    /**
     * Executes the task with a map of parameters returning {@link Execution}
     * object that can be used by the caller to analyze the results. Currently
     * all task implementations are synchronous, so this method returns only on
     * task completion.
     *
     * @since 1.3
     * @deprecated as we are no longer planning to support {@link SyncToken}
     */
    @Deprecated(since = "3.0.0", forRemoval = true)
    Execution run(SyncToken token, Map<String, ?> params);

}
