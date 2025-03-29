package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.runtime.task.common.DataSegment;
import org.apache.cayenne.ObjectContext;
import org.dflib.DataFrame;

/**
 * @since 2.6
 */
public class CreateSegment extends DataSegment<CreateStage> {

    public static final String TARGET_COLUMN = "$lm_target";
    public static final String TARGET_CREATED_COLUMN = "$lm_target_created";

    private final ObjectContext context;
    private final RowAttribute[] sourceRowsHeader;

    public CreateSegment(ObjectContext context, RowAttribute[] sourceRowsHeader) {
        this.sourceRowsHeader = sourceRowsHeader;
        this.context = context;
    }

    public ObjectContext getContext() {
        return context;
    }

    public RowAttribute[] getSourceRowsHeader() {
        return sourceRowsHeader;
    }

    public DataFrame getSourceRows() {
        return get(CreateStage.EXTRACT_SOURCE_ROWS);
    }

    /**
     * @since 2.17
     */
    public CreateSegment setSourceRows(DataFrame df) {
        set(CreateStage.EXTRACT_SOURCE_ROWS, df);
        return this;
    }

    public DataFrame getSources() {
        return get(CreateStage.CONVERT_SOURCE_ROWS);
    }

    public CreateSegment setSources(DataFrame df) {
        set(CreateStage.CONVERT_SOURCE_ROWS, df);
        return this;
    }

    public DataFrame getMapped() {
        return get(CreateStage.MAP_TARGET);
    }

    public CreateSegment setMapped(DataFrame df) {
        set(CreateStage.MAP_TARGET, df);
        return this;
    }

    /**
     * @since 2.12
     */
    public DataFrame getFksResolved() {
        return get(CreateStage.RESOLVE_FK_VALUES);
    }

    /**
     * @since 2.12
     */
    public CreateSegment setFksResolved(DataFrame df) {
        set(CreateStage.RESOLVE_FK_VALUES, df);
        return this;
    }

    public DataFrame getMerged() {
        return get(CreateStage.MERGE_TARGET);
    }

    public CreateSegment setMerged(DataFrame df) {
        set(CreateStage.MERGE_TARGET, df);
        return this;
    }
}
