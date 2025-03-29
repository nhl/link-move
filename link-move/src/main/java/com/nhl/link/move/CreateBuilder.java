package com.nhl.link.move;

import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.runtime.task.create.CreateSegment;
import com.nhl.link.move.runtime.task.create.CreateStage;
import org.dflib.DataFrame;

import java.util.function.BiConsumer;
import java.util.function.Function;

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
     * Adds a callback invoked for each data segment after the specified stage in the "create" processing pipeline.
     *
     * @since 3.0.0
     */
    CreateBuilder stage(CreateStage stage, BiConsumer<Execution, CreateSegment> callback);

    /**
     * Adds a callback invoked for each date segment after the specified stage in the "create" pipeline was processed.
     * The result of that stage is passed to the transformer argument. The value returned from the transformer overrides
     * the previous result for the stage.
     *
     * @since 4.0.0
     */
    default CreateBuilder stage(CreateStage stage, Function<DataFrame, DataFrame> transformer) {
        return stage(stage, (e, s) -> s.postProcess(stage, transformer));
    }
}
