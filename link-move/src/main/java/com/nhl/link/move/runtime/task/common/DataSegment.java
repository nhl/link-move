package com.nhl.link.move.runtime.task.common;

import org.dflib.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public abstract class DataSegment<U extends TaskStageType> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSegment.class);

    protected final Map<U, DataFrame> stageResults;

    public DataSegment() {
        // while results are mutable, stages are sequential (and DataSegment is assumed to be single-threaded), so no
        // need for concurrency
        this.stageResults = new HashMap<>();
    }

    /**
     * @since 4.0.0
     */
    public DataFrame get(U stage) {
        return stageResults.get(stage);
    }

    /**
     * @since 4.0.0
     */
    public void set(U stage, DataFrame df) {
        stageResults.put(stage, df);
    }

    /**
     * Retrieves data from the specified stage and, if it is present, applies the processor and saves the result back
     * replacing the previous result of this stage.
     *
     * @since 4.0.0
     */
    public void postProcess(U stage, Function<DataFrame, DataFrame> processor) {
        DataFrame df = get(stage);
        if (df != null) {
            set(stage, processor.apply(df));
        } else {
            LOGGER.warn("Skipping postprocessing. Stage {} has no results", stage);
        }
    }
}
