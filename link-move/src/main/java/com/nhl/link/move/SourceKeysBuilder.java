package com.nhl.link.move;

import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysSegment;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysStage;
import org.dflib.DataFrame;

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A builder of an {@link LmTask} that extracts all the keys from the source
 * data store.
 *
 * @since 1.3
 */
public interface SourceKeysBuilder {

    /**
     * Creates a new task based on the builder information.
     */
    LmTask task() throws IllegalStateException;

    /**
     * Defines the location and name of the source data extractor.
     *
     * @since 3.0.0
     */
    SourceKeysBuilder sourceExtractor(ExtractorName extractorName);

    /**
     * Defines the location and name of the source data extractor.
     *
     * @since 1.4
     */
    default SourceKeysBuilder sourceExtractor(String location, String name) {
        return sourceExtractor(ExtractorName.create(location, name));
    }

    /**
     * Defines the location of the source data extractor. The name of extractor
     * is assumed to be "default_extractor".
     *
     * @since 1.3
     */
    default SourceKeysBuilder sourceExtractor(String location) {
        return sourceExtractor(ExtractorName.create(location, ExtractorName.DEFAULT_NAME));
    }

    /**
     * Defines the number of records that are processed together as a single
     * batch. If not specified, default size of 500 records is used.
     */
    SourceKeysBuilder batchSize(int batchSize);

    SourceKeysBuilder matchBy(Mapper mapper);

    SourceKeysBuilder matchBy(String... columns);

    /**
     * Adds a callback invoked for each processed segment after the specified stage in the "source keys" extraction
     * pipeline.
     *
     * @since 3.0.0
     */
    SourceKeysBuilder stage(SourceKeysStage stage, BiConsumer<Execution, SourceKeysSegment> callback);

    /**
     * Adds a callback invoked for each date segment after the specified stage in the "source keys" pipeline was processed.
     * The result of that stage is passed to the transformer argument. The value returned from the transformer overrides
     * the previous result for the stage.
     *
     * @since 4.0.0
     */
    default SourceKeysBuilder stage(SourceKeysStage stage, Function<DataFrame, DataFrame> transformer) {
        return stage(stage, (e, s) -> s.postProcess(stage, transformer));
    }
}
