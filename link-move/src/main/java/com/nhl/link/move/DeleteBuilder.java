package com.nhl.link.move;

import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.runtime.task.delete.DeleteSegment;
import com.nhl.link.move.runtime.task.delete.DeleteStage;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.property.Property;
import org.dflib.DataFrame;

import java.util.function.BiConsumer;
import java.util.function.Function;

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
        return sourceMatchExtractor(location, ExtractorName.DEFAULT_NAME);
    }

    /**
     * Defines the location and name of the target data extractor.
     *
     * @param location extractor configuration location, relative to some root known to LinkMove.
     * @param name     extractor name within configuration.
     * @return this builder instance
     * @since 3.0.0
     */
    DeleteBuilder sourceMatchExtractor(String location, String name);

    DeleteBuilder matchBy(Mapper mapper);

    DeleteBuilder matchBy(String... keyAttributes);

    DeleteBuilder matchBy(Property<?>... keyAttributes);

    DeleteBuilder matchById();

    DeleteBuilder batchSize(int batchSize);

    /**
     * Adds a callback invoked for each processed segment after the specified stage in the "delete" processing pipeline.
     *
     * @since 3.0.0
     */
    DeleteBuilder stage(DeleteStage stage, BiConsumer<Execution, DeleteSegment> callback);

    /**
     * Adds a callback invoked for each date segment after the specified stage in the "delete" pipeline was processed.
     * The result of that stage is passed to the transformer argument. The value returned from the transformer overrides
     * the previous result for the stage.
     *
     * @since 4.0.0
     */
    default DeleteBuilder stage(DeleteStage stage, Function<DataFrame, DataFrame> transformer) {
        return stage(stage, (e, s) -> s.postProcess(stage, transformer));
    }
}
