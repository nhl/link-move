package com.nhl.link.move;

import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateSegment;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateStage;
import org.apache.cayenne.exp.property.Property;

import java.util.function.BiConsumer;

/**
 * A builder of an {@link LmTask} that performs create-or-update synchronization.
 *
 * @since 1.3
 */
public interface CreateOrUpdateBuilder {

    /**
     * Creates a new task based on the builder information.
     *
     * @return a new task based on the builder information.
     */
    LmTask task() throws IllegalStateException;

    /**
     * Defines the location and name of the source data extractor.
     *
     * @param location extractor configuration location, relative to some root known
     *                 to LinkMove.
     * @param name     extractor name within configuration.
     * @return this builder instance
     * @since 1.4
     */
    CreateOrUpdateBuilder sourceExtractor(String location, String name);

    /**
     * Defines the location of the source data extractor. The name of extractor is assumed to be "default_extractor".
     *
     * @param location extractor configuration location, relative to some root known to LinkMove.
     * @return this builder instance
     * @see #sourceExtractor(String, String)
     * @since 1.3
     */
    default CreateOrUpdateBuilder sourceExtractor(String location) {
        return sourceExtractor(location, ExtractorName.DEFAULT_NAME);
    }

    /**
     * Instructs the task to match sources and targets using explicitly provided {@link Mapper}.
     *
     * @param mapper a custom {@link Mapper} to match sources against targets.
     * @return this builder instance
     */
    CreateOrUpdateBuilder matchBy(Mapper mapper);

    /**
     * Instructs the task to match sources and targets based on one or more attributes.
     *
     * @param keyAttributes target attributes to use in source-to-target mapper.
     * @return this builder instance
     */
    CreateOrUpdateBuilder matchBy(String... keyAttributes);

    /**
     * Instructs the task to match sources and targets based on one or more Persistent properties.
     *
     * @param keyAttributes target attributes to use in source-to-target mapper.
     * @return this builder instance
     */
    CreateOrUpdateBuilder matchBy(Property<?>... keyAttributes);

    CreateOrUpdateBuilder matchById();

    /**
     * Defines the number of records that are processed together as a single
     * batch. If not specified, default size of 500 records is used.
     *
     * @param batchSize the size of an internal processing batch.
     * @return this builder instance
     * @since 1.3
     */
    CreateOrUpdateBuilder batchSize(int batchSize);

    /**
     * Adds a callback invoked for each processed segment after the specified stage in the "create-or-update" processing
     * pipeline.
     *
     * @since 3.0.0
     */
    CreateOrUpdateBuilder stage(CreateOrUpdateStage stageType, BiConsumer<Execution, CreateOrUpdateSegment> callback);
}
