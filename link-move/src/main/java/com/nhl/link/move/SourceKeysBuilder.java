package com.nhl.link.move;

import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysSegment;
import com.nhl.link.move.runtime.task.sourcekeys.SourceKeysStage;

import java.util.function.BiConsumer;

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
     * @since 3.0
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
     * Adds a listener of transformation stages of batch segments. It should have methods annotated with
     * {@link com.nhl.link.move.annotation.AfterSourceRowsExtracted}. Annotated method
     * signature should be as follows:
     *
     * <pre>
     * @AfterSourceRowsExtracted
     * public void method(Execution e, CreateOrUpdateSegment<T> s) {}
     * </pre>
     *
     * @param listener an annotated object that will receive events as the task proceeds.
     * @return this builder instance
     * @since 3.0
     *
     * @deprecated use lambda-based callbacks instead, @see {@link com.nhl.link.move.SourceKeysBuilder#stage}
     */
    @Deprecated(since = "3.0")
    SourceKeysBuilder stageListener(Object listener);

    SourceKeysBuilder stage(SourceKeysStage stageType, BiConsumer<Execution, SourceKeysSegment> callback);
}
