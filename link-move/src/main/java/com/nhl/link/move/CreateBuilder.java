package com.nhl.link.move;

import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.task.create.CreateSegment;
import com.nhl.link.move.runtime.task.create.CreateStage;

import java.util.function.BiConsumer;

/**
 * A builder of an {@link LmTask} that performs fast "create" synchronization without any source/target key matching.
 *
 * @since 2.6
 */
public interface CreateBuilder {

    /**
     * Creates a new task based on the builder information.
     *
     * @return a new task based on the builder information.
     */
    LmTask task() throws IllegalStateException;

    /**
     * Defines the location and name of the source data extractor.
     *
     * @param location extractor configuration location, relative to some root known to LinkMove.
     * @param name     extractor name within configuration.
     * @return this builder instance
     */
    CreateBuilder sourceExtractor(String location, String name);

    /**
     * Defines the location of the source data extractor. The name of extractor is assumed to be "default_extractor".
     *
     * @param location extractor configuration location, relative to some root known to LinkMove.
     * @return this builder instance
     * @see #sourceExtractor(String, String)
     */
    default CreateBuilder sourceExtractor(String location) {
        return sourceExtractor(location, ExtractorName.DEFAULT_NAME);
    }

    /**
     * Defines the number of records that are processed together as a single batch. If not specified, default size of
     * 500 records is used.
     *
     * @param batchSize the size of an internal processing batch.
     * @return this builder instance
     */
    CreateBuilder batchSize(int batchSize);

    /**
     * Adds a listener of transformation stages of batch segments. It should have methods annotated with
     * {@link com.nhl.link.move.annotation.AfterSourceRowsExtracted}, {@link AfterSourceRowsConverted},
     * {@link AfterTargetsMapped}, {@link AfterTargetsMerged}, {@link AfterTargetsCommitted}, etc. Annotated method
     * signature should be as follows:
     *
     * <pre>
     * @AfterXyz
     * public void method(Execution e, CreateOrUpdateSegment<T> s) {}
     * </pre>
     *
     * @param listener an annotated object that will receive events as the task proceeds.
     * @return this builder instance
     *
     * @deprecated use lambda-based callbacks instead, @see {@link com.nhl.link.move.CreateBuilder#stage}
     */
    @Deprecated(since = "3.0")
    CreateBuilder stageListener(Object listener);

    CreateBuilder stage(CreateStage stageType, BiConsumer<Execution, CreateSegment> callback);
}
