package com.nhl.link.move;

import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.runtime.task.delete.DeleteSegment;
import com.nhl.link.move.runtime.task.delete.DeleteStage;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.property.Property;

import java.util.function.BiConsumer;

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
     * @since 3.0
     */
    DeleteBuilder sourceMatchExtractor(String location, String name);

    DeleteBuilder matchBy(Mapper mapper);

    DeleteBuilder matchBy(String... keyAttributes);

    DeleteBuilder matchBy(Property<?>... keyAttributes);

    DeleteBuilder matchById();

    DeleteBuilder batchSize(int batchSize);

    /**
     * @deprecated use lambda-based callbacks instead, @see {@link com.nhl.link.move.DeleteBuilder#stage}
     */
    @Deprecated(since = "3.0")
    DeleteBuilder stageListener(Object listener);

    DeleteBuilder stage(DeleteStage stageType, BiConsumer<Execution, DeleteSegment> callback);
}
