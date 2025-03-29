package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.runtime.task.common.DataSegment;
import org.apache.cayenne.ObjectContext;
import org.dflib.DataFrame;

import java.util.Set;

public class DeleteSegment extends DataSegment<DeleteStage> {

    public static final String TARGET_COLUMN = "$lm_target";
    public static final String KEY_COLUMN = "$lm_key";

    private final ObjectContext context;
    private final Set<Object> sourceKeys;

    /**
     * @since 4.0.0
     */
    public DeleteSegment(ObjectContext context, Set<Object> sourceKeys) {
        this.context = context;
        this.sourceKeys = sourceKeys;
    }

    public ObjectContext getContext() {
        return context;
    }

    /**
     * @since 1.6
     */
    public Set<Object> getSourceKeys() {
        return sourceKeys;
    }

    public DataFrame getTargets() {
        return get(DeleteStage.EXTRACT_TARGET);
    }

    /**
     * @since 3.0.0
     */
    public DeleteSegment setTargets(DataFrame df) {
        set(DeleteStage.EXTRACT_TARGET, df);
        return this;
    }

    public DataFrame getMappedTargets() {
        return get(DeleteStage.MAP_TARGET);
    }

    public DeleteSegment setMappedTargets(DataFrame df) {
        set(DeleteStage.MAP_TARGET, df);
        return this;
    }

    public DataFrame getMissingTargets() {
        return get(DeleteStage.FILTER_MISSING_TARGETS);
    }

    public DeleteSegment setMissingTargets(DataFrame df) {
        set(DeleteStage.FILTER_MISSING_TARGETS, df);
        return this;
    }

    /**
     * @since 4.0.0
     */
    public DataFrame getDeletedTargets() {
        return get(DeleteStage.DELETE_TARGET);
    }

    /**
     * @since 4.0.0
     */
    public DeleteSegment setDeletedTargets(DataFrame df) {
        set(DeleteStage.DELETE_TARGET, df);
        return this;
    }
}
