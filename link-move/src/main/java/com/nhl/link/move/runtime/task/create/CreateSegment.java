package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.RowAttribute;
import com.nhl.dflib.DataFrame;
import com.nhl.link.move.runtime.task.common.DataSegment;
import org.apache.cayenne.ObjectContext;

/**
 * @since 2.6
 */
public class CreateSegment implements DataSegment {

    public static final String TARGET_COLUMN = "$lm_target";
    public static final String TARGET_CREATED_COLUMN = "$lm_target_created";

    private final ObjectContext context;
    private final RowAttribute[] sourceRowsHeader;

    private DataFrame sourceRows;
    private DataFrame sources;
    private DataFrame mapped;
    private DataFrame fksResolved;
    private DataFrame merged;

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
        return sourceRows;
    }

    /**
     * @since 2.17
     */
    public CreateSegment setSourceRows(DataFrame sourceRows) {
        this.sourceRows = sourceRows;
        return this;
    }

    public DataFrame getSources() {
        return sources;
    }

    public CreateSegment setSources(DataFrame translatedSegment) {
        this.sources = translatedSegment;
        return this;
    }

    public DataFrame getMerged() {
        return merged;
    }

    public CreateSegment setMerged(DataFrame merged) {
        this.merged = merged;
        return this;
    }

    public DataFrame getMapped() {
        return mapped;
    }

    public CreateSegment setMapped(DataFrame mapped) {
        this.mapped = mapped;
        return this;
    }

    /**
     * @since 2.12
     */
    public DataFrame getFksResolved() {
        return fksResolved;
    }

    /**
     * @since 2.12
     */
    public CreateSegment setFksResolved(DataFrame fksResolved) {
        this.fksResolved = fksResolved;
        return this;
    }
}
