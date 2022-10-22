package com.nhl.link.move;

import com.nhl.link.move.extractor.model.ExtractorModel;
import org.apache.cayenne.exp.Expression;

import com.nhl.link.move.mapper.Mapper;
import org.apache.cayenne.exp.property.Property;

/**
 * since 1.3
 */
public interface DeleteBuilder {

    /**
     * Creates a new task that will delete target objects not found in the source.
     */
    LmTask task() throws IllegalStateException;

    DeleteBuilder targetFilter(Expression filter);

    default DeleteBuilder sourceMatchExtractor(String location) {
        return sourceMatchExtractor(location, ExtractorModel.DEFAULT_NAME);
    }

    /**
     * Defines the location and name of the target data extractor.
     *
     * @param location extractor configuration location, relative to some root known to LinkMove.
     * @param name     extractor name within configuration.
     * @return this builder instance
     * @since 3.0
     */
    DeleteBuilder sourceMatchExtractor(String location, String name);

    DeleteBuilder matchBy(Mapper mapper);

    DeleteBuilder matchBy(String... keyAttributes);

    DeleteBuilder matchBy(Property<?>... keyAttributes);

    DeleteBuilder matchById();

    DeleteBuilder batchSize(int batchSize);

    DeleteBuilder stageListener(Object listener);
}
